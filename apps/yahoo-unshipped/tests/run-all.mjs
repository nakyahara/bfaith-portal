/**
 * Yahoo! 未発送アラート のテスト。
 *   node apps/yahoo-unshipped/tests/run-all.mjs
 * Yahoo API にも warehouse.db にもアクセスしない (判定・整形の純関数だけを検証する)。
 */
import { parseStringPromise } from 'xml2js';
import {
  addDaysStr,
  parseYahooDatetime,
  buildContext,
  extractOrderInfo,
  isStillUnshipped,
  verifyCandidates,
  buildMessage,
  loadResolved,
  saveResolved,
  resolvedFilePath,
  MAX_LINES,
} from '../service.js';
import { exitCodeFor } from '../notify-job.js';

// verifyCandidates は fetch を差し替えて呼ぶが、プロキシ設定の検証はその手前で走るため
// ダミー値を入れておく (実際の通信は fetchImpl のスタブが受ける)
process.env.YAHOO_PROXY_SECRET = process.env.YAHOO_PROXY_SECRET || 'test-secret';
process.env.YAHOO_PROXY_URL = 'http://proxy.invalid';

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
section('addDaysStr / parseYahooDatetime');
eq(addDaysStr('2026-08-10', -1), '2026-08-09', '1日前');
eq(addDaysStr('2026-03-01', -1), '2026-02-28', '月またぎ');
eq(addDaysStr('2026-01-01', -1), '2025-12-31', '年またぎ');
eq(parseYahooDatetime('2026-08-06T17:16:34+09:00')?.toISOString(), '2026-08-06T08:16:34.000Z', 'Yahooの日時形式');
eq(parseYahooDatetime(''), null, '空文字');
eq(parseYahooDatetime(null), null, 'null');
eq(parseYahooDatetime('壊れた値'), null, '不正な文字列');

// ─── buildContext ───
section('buildContext (JST基準)');
{
  // miniPC は JST だが、UTC 実行でも JST の日付になること
  const ctx = buildContext(new Date('2026-08-09T23:00:00Z'), 12); // = 8/10 08:00 JST
  eq(ctx.today, '2026-08-10', 'UTC実行でも today は JST');
  eq(ctx.cutoffDate, '2026-08-09', '締め日 = 前日');
  eq(ctx.cutoffStr, '2026-08-09T12:00:00+09:00', 'DB比較用の文字列はDBと同じ +09:00 付き');
  eq(ctx.cutoff.toISOString(), '2026-08-09T03:00:00.000Z', '締め = 前日12:00 JST');
}
{
  const ctx = buildContext(new Date('2026-08-01T00:30:00+09:00'), 12);
  eq(ctx.cutoffDate, '2026-07-31', '月初の締め日は前月末');
}
// DBの order_time は '2026-08-06T17:16:34+09:00' 形式。cutoffStr との文字列比較が
// 正しい大小関係になること (ここが崩れると SQL の絞り込みが黙って壊れる)
section('DB文字列比較の前提');
const CUT = '2026-08-09T12:00:00+09:00';
ok('2026-08-06T17:16:34+09:00' <= CUT, '締め前の注文は cutoffStr 以下');
ok(!('2026-08-09T12:00:01+09:00' <= CUT), '締め1秒後は cutoffStr を超える');
ok('2026-08-09T11:59:59+09:00' <= CUT, '締め1秒前は cutoffStr 以下');
// 🚨オフセットを省くと '...12:00:00+09:00' > '...12:00:00' となり、ちょうど締め時刻の注文が漏れる
ok('2026-08-09T12:00:00+09:00' <= CUT, '締めちょうどの注文が含まれる (オフセット付きで揃えた効果)');
ok(!('2026-08-09T12:00:00+09:00' <= '2026-08-09T12:00:00'), '(参考) オフセット無しだと締めちょうどが漏れる');

// ─── extractOrderInfo (実際のXML構造) ───
section('extractOrderInfo');
const XML = `<?xml version='1.0' encoding='UTF-8'?><ResultSet totalResultsAvailable="1"><Result><Status>OK</Status>
<OrderInfo><Detail><Discount>0</Discount><PayCharge>0</PayCharge><ShipCharge>0</ShipCharge><TotalPrice>698</TotalPrice><UsePoint>0</UsePoint></Detail>
<LastUpdateTime>2026-08-06T17:32:06+09:00</LastUpdateTime>
<Item><LineId>1</LineId><Title>カーテン用すそ上げテープ</Title><Quantity>2</Quantity><ItemId>abc</ItemId></Item>
<OrderId>b-faith01-10285999</OrderId><OrderStatus>2</OrderStatus><OrderTime>2026-08-06T17:16:34+09:00</OrderTime>
<Pay><PayStatus>1</PayStatus></Pay><Ship><ShipStatus>1</ShipStatus></Ship></OrderInfo></Result></ResultSet>`;
{
  const info = extractOrderInfo(await parseStringPromise(XML, { explicitArray: false }));
  eq(info.orderId, 'b-faith01-10285999', '注文ID');
  eq(info.orderStatus, '2', 'OrderStatus (OrderInfo直下)');
  eq(info.payStatus, '1', 'PayStatus (Pay配下にネスト)');
  eq(info.shipStatus, '1', 'ShipStatus (Ship配下にネスト)');
  eq(info.totalPrice, 698, 'TotalPrice (Detail配下)');
  eq(info.firstItemName, 'カーテン用すそ上げテープ', '商品名');
  eq(info.itemCount, 1, '明細数 (単一Itemでも配列化)');
  eq(info.totalUnits, 2, '数量');
  ok(!JSON.stringify(info).includes('住所') && !('Ship' in info), '個人情報セクションを持ち回らない');
}
{
  const multi = XML.replace('</Item>', '</Item><Item><LineId>2</LineId><Title>B</Title><Quantity>3</Quantity></Item>');
  const info = extractOrderInfo(await parseStringPromise(multi, { explicitArray: false }));
  eq(info.itemCount, 2, '複数明細');
  eq(info.totalUnits, 5, '数量の合算');
}
eq(extractOrderInfo({}), null, '空レスポンスは null');
eq(extractOrderInfo(await parseStringPromise('<ResultSet><Result><Status>OK</Status></Result></ResultSet>', { explicitArray: false })), null, 'OrderInfo が無ければ null');

// ─── isStillUnshipped ───
section('isStillUnshipped (API最新値での確定判定)');
const info = (over = {}) => ({ orderStatus: '2', payStatus: '1', shipStatus: '1', ...over });
eq(isStillUnshipped(info()), true, '処理中 × 入金済み × 出荷可能 → 未発送');
eq(isStillUnshipped(info({ shipStatus: '3' })), false, '出荷済み(3) → 対象外');
eq(isStillUnshipped(info({ shipStatus: '0' })), false, '出荷不可(0) → 対象外');
eq(isStillUnshipped(info({ orderStatus: '4' })), false, 'キャンセル(4) → 対象外');
eq(isStillUnshipped(info({ orderStatus: '5', shipStatus: '3' })), false, '完了+出荷済み → 対象外');
eq(isStillUnshipped(info({ payStatus: '0' })), false, '未入金 → 対象外 (出荷できなくて当然)');
eq(isStillUnshipped(null), false, 'null でも落ちない');
// 実測: DB候補9件のうち7件がこの判定で落ちた (5件キャンセル・2件出荷済み)
eq(isStillUnshipped(info({ orderStatus: '4', shipStatus: '1' })), false,
  '🚨DBが古いパターン: キャンセル済みなのに ship=1 のまま → 落とす');

// ─── verifyCandidates (fetchを差し替えて検証) ───
section('verifyCandidates');
const ctxNow = { ...buildContext(new Date('2026-08-10T08:00:00+09:00'), 12), now: new Date('2026-08-10T08:00:00+09:00') };
const cand = (id, t) => ({ order_id: id, order_time: t, last_update_time: t, order_status: '2', ship_status: '1', total_price: 698 });
const fakeFetch = (results) => async () => ({ ok: true, json: async () => ({ ok: true, results }) });
{
  const shipped = XML.replace('<ShipStatus>1</ShipStatus>', '<ShipStatus>3</ShipStatus>');
  const r = await verifyCandidates(
    [cand('b-faith01-10285999', '2026-08-06T17:16:34+09:00'), cand('X2', '2026-08-05T10:00:00+09:00')],
    ctxNow,
    { fetchImpl: fakeFetch([{ orderId: 'b-faith01-10285999', xml: XML }, { orderId: 'X2', xml: shipped }]) },
  );
  eq(r.alerts.length, 1, '出荷済みになっていた候補は落ちる');
  eq(r.alerts[0].orderId, 'b-faith01-10285999', '残るのは未発送のみ');
  eq(r.verified, 2, '確認した件数');
  eq(r.apiFailed, 0, '確認失敗なし');
  eq(r.alerts[0].elapsedHours, 86.7, '注文からの経過時間');
  eq(r.alerts[0].shipStatusLabel, '出荷可能', 'ステータス名');
}
{
  // APIが返してこなかった注文は apiFailed に数える (黙って「解消」にしない)
  const r = await verifyCandidates([cand('A', '2026-08-05T10:00:00+09:00')], ctxNow, { fetchImpl: fakeFetch([]) });
  eq(r.alerts.length, 0, '確認できなければ通知しない');
  eq(r.apiFailed, 1, '確認できなかった件数を数える');
}
{
  const many = Array.from({ length: 5 }, (_, i) => cand(`A${i}`, '2026-08-05T10:00:00+09:00'));
  const r = await verifyCandidates(many, ctxNow, { maxVerify: 2, fetchImpl: fakeFetch([]) });
  eq(r.verified, 2, '上限で打ち切る');
  eq(r.truncated, true, '打ち切りを記録する');
}
{
  const r = await verifyCandidates([], ctxNow, { fetchImpl: fakeFetch([]) });
  eq(r.alerts.length, 0, '候補0件ならAPIを呼ばない');
  eq(r.verified, 0, '確認件数0');
}
{
  let called = false;
  const errFetch = async () => { called = true; return { ok: false, status: 503, text: async () => 'unavailable' }; };
  let threw = false;
  try { await verifyCandidates([cand('A', '2026-08-05T10:00:00+09:00')], ctxNow, { fetchImpl: errFetch }); }
  catch (e) { threw = /503/.test(e.message); }
  ok(called && threw, 'プロキシエラーは握り潰さず throw する');
}

// ─── buildMessage ───
section('buildMessage');
const mkAlert = (over = {}) => ({
  orderId: 'b-faith01-10285999',
  orderedAt: new Date('2026-08-06T17:16:34+09:00'),
  updatedAt: new Date('2026-08-06T17:32:06+09:00'),
  elapsedHours: 86.7,
  orderStatusLabel: '処理中',
  shipStatusLabel: '出荷可能',
  totalPrice: 698,
  itemCount: 1,
  firstItemName: 'カーテン用すそ上げテープ',
  ...over,
});
{
  const text = buildMessage({ alerts: [], candidates: 9, apiFailed: 0, truncated: false, ctx: ctxNow });
  ok(text.includes('*Yahoo! 未発送アラート*'), 'モール名が分かる見出し');
  ok(text.includes('✅ 出荷漏れはありません'), '0件でも本文を作る');
  ok(text.includes('候補9件はすべて出荷済み・キャンセル済み'), '候補があった日はその旨を出す');
  ok(text.includes('08/09 12:00'), '締め時刻 (前日12:00) を明記');
}
{
  const text = buildMessage({ alerts: [mkAlert()], candidates: 3, apiFailed: 0, truncated: false, ctx: ctxNow });
  ok(text.includes('🚨 出荷漏れの可能性 *1件*'), '件数');
  ok(text.includes('b-faith01-10285999'), '注文ID');
  ok(text.includes('¥698'), '金額');
  ok(text.includes('86.7時間経過'), '経過時間');
  ok(text.includes('[処理中/出荷可能]'), 'ステータス');
  ok(text.includes('8/6 17:16'), '注文日時');
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
  const text = buildMessage({ alerts: [], recent: [mkAlert()], candidates: 3, apiFailed: 0, truncated: false, ctx: ctxNow });
  ok(text.includes('🕒 未発送だが締め後に動きあり 1件'), '参考枠を本文に出す');
  ok(text.includes('今朝の入金・変更かもしれません'), '参考枠の意味を書く');
  ok(text.includes('✅ 出荷漏れはありません'), '🚨が0件なら0件と明記する');
}
{
  const long = mkAlert({ firstItemName: 'あ'.repeat(80), itemCount: 3 });
  const text = buildMessage({ alerts: [long], candidates: 1, apiFailed: 0, truncated: false, ctx: ctxNow });
  ok(text.includes('…'), '長い商品名は切る');
  ok(text.includes('ほか2点'), '2件目以降は点数で表す');
}

// ─── 締め後に動きがある注文の切り分け (Codexレビュー High) ───
section('締め後に動きがある注文は落とさず参考枠へ');
{
  // LastUpdateTime は入金日時ではない。締め前に入金済みでも、締め後に住所変更などが
  // あれば更新される。これで候補から外すと出荷漏れを見逃すため、参考枠に回す
  const movedXml = XML.replace('<LastUpdateTime>2026-08-06T17:32:06+09:00</LastUpdateTime>',
    '<LastUpdateTime>2026-08-10T07:00:00+09:00</LastUpdateTime>');
  const r = await verifyCandidates(
    [cand('b-faith01-10285999', '2026-08-06T17:16:34+09:00')],
    ctxNow,
    { fetchImpl: fakeFetch([{ orderId: 'b-faith01-10285999', xml: movedXml }]) },
  );
  eq(r.alerts.length, 0, '締め後に更新があれば 🚨 には出さない');
  eq(r.recent.length, 1, '参考枠には残す (見逃さない)');
  eq(r.recent[0].orderId, 'b-faith01-10285999', '参考枠の中身');
}
{
  const r = await verifyCandidates(
    [cand('b-faith01-10285999', '2026-08-06T17:16:34+09:00')],
    ctxNow,
    { fetchImpl: fakeFetch([{ orderId: 'b-faith01-10285999', xml: XML }]) },
  );
  eq(r.alerts.length, 1, '締め前の更新なら 🚨 に出す');
  eq(r.recent.length, 0, '参考枠は空');
}
{
  // 解消済み (出荷済み・キャンセル) は resolvedIds に入れて次回の候補から外す
  const shipped = XML.replace('<ShipStatus>1</ShipStatus>', '<ShipStatus>3</ShipStatus>');
  const r = await verifyCandidates([cand('S1', '2026-08-05T10:00:00+09:00')], ctxNow,
    { fetchImpl: fakeFetch([{ orderId: 'S1', xml: shipped }]) });
  eq(r.resolvedIds, ['S1'], '解消済みは記録して次回スキップできるようにする');
}
{
  const r = await verifyCandidates([cand('F1', '2026-08-05T10:00:00+09:00')], ctxNow, { fetchImpl: fakeFetch([]) });
  eq(r.resolvedIds, [], '確認できなかった注文は「解消済み」に入れない (次回また見る)');
}

// ─── 解消済みキャッシュ ───
section('解消済みキャッシュ');
{
  const os = await import('node:os');
  const fs = await import('node:fs');
  const path = await import('node:path');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'yahoo-unshipped-'));
  const prev = process.env.DATA_DIR;
  process.env.DATA_DIR = tmp;
  try {
    eq(loadResolved(), {}, 'ファイルが無ければ空');
    saveResolved({ A: '2026-08-10', B: '2026-01-01' }, '2026-08-10');
    const loaded = loadResolved();
    eq(loaded.A, '2026-08-10', '新しい記録は残る');
    eq(loaded.B, undefined, '保持日数を過ぎた記録は捨てる');
    fs.writeFileSync(resolvedFilePath(), '{壊れたJSON');
    eq(loadResolved(), {}, '壊れていても空で続行する (判定は止めない)');
    fs.writeFileSync(resolvedFilePath(), '[1,2,3]');
    eq(loadResolved(), {}, '配列など想定外の形も空扱い');
  } finally {
    if (prev === undefined) delete process.env.DATA_DIR; else process.env.DATA_DIR = prev;
    fs.rmSync(tmp, { recursive: true, force: true });
  }
}

// ─── exitCodeFor ───
section('exitCodeFor');
eq(exitCodeFor({ apiFailed: 0, truncated: false }), 0, '完全 → 0');
eq(exitCodeFor({ apiFailed: 2, truncated: false }), 1, '確認できない注文がある → 1 (当日中にretryする)');
eq(exitCodeFor({ apiFailed: 0, truncated: true }), 2, '上限打ち切り → 2 (retryしても同じ = blocked)');
eq(exitCodeFor({ apiFailed: 1, truncated: true }), 1, '両方なら retry を優先する');
eq(exitCodeFor({}), 0, '空でも 0');
eq(exitCodeFor(null), 0, 'null でも落ちない');

console.log(`\n${fail === 0 ? '全テスト pass' : `${fail}件 失敗`} (pass=${pass} fail=${fail})`);
process.exit(fail === 0 ? 0 : 1);
