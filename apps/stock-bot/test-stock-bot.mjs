/**
 * test-stock-bot.mjs — Google Chat 在庫検索ボットのロジック検証
 *
 * mirror_logizard_stock を一時DBに作り、検索・カード生成・ボタン選択・整形を検証する。
 * (Bearer検証はGoogle署名が必要なため対象外 = 実機で確認)
 *
 * 実行: node apps/stock-bot/test-stock-bot.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'stockbot-test-'));
process.env.DATA_DIR = tmpDir;

const { initMirrorDB, getMirrorDB } = await import('../warehouse-mirror/db.js');
const routerMod = await import('./router.js');
const { searchProducts, buildStockReply, handleChatEvent, makeStockBotAuth, checkRateLimit } = routerMod;
const stockBotRouter = routerMod.default;

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

initMirrorDB();
const db = getMirrorDB();
const NOW = new Date('2026-08-16T07:30:00Z');   // JST 16:30
const CAPTURED = '2026-08-16T07:00:00.000Z';    // JST 16:00 (30分前=新鮮)

const ins = db.prepare(`INSERT INTO mirror_logizard_stock (
  商品ID, 商品名, バーコード, ブロック略称, ロケ, 品質区分名, 有効期限, 入荷日,
  在庫数, 引当数, ロケ業務区分, 最終入荷日, 最終出荷日, 在庫日, captured_at, synced_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
const put = (sku, name, barcode, block, loc, quality, qty, alloc, expiry = '') =>
  ins.run(sku, name, barcode, block, loc, quality, expiry, '', qty, alloc, '', '', '', '20260816', CAPTURED, '2026-08-16 16:00:00');

put('teatree10', 'ティーツリーオイル 10ml', 'X000TEA10', 'R1FA', '001-001-01', '良品', 50, 10, '20280115');
put('teatree20', 'ティーツリーオイル 20ml', 'X000TEA20', 'R1FA', '002-001-01', '良品', 200, 0);
put('teatree20', 'ティーツリーオイル 20ml', 'X000TEA20', 'P1FB', '001-002-01', '良品', 30, 0);
put('teatree20', 'ティーツリーオイル 20ml', 'X000TEA20', 'ZZZ', 'ZZZ-ZZZ-ZZ', '良品', 15, 0);
put('teatree20', 'ティーツリーオイル 20ml', 'X000TEA20', 'P2FA', '003-001-01', '不良品', 99, 0);
for (let i = 1; i <= 12; i++) put(`hakka${i}`, `ハッカ油テスト${i}`, `X000HK${i}`, 'R1FA', `00${i}-001-01`.slice(-10), '良品', i, 0);

console.log('\n── searchProducts ──');
{
  const r = searchProducts(db, 'ティーツリー');
  eq(r.map((x) => x.sku), ['teatree20', 'teatree10'], '部分一致・良品フリー降順 (20ml=245 > 10ml=40)');
  eq(r[0].free, 245, '良品のみ合算 (不良品99は含めない)');
  eq(searchProducts(db, 'X000TEA20').map((x) => x.sku), ['teatree20'], 'バーコード完全一致');
  eq(searchProducts(db, 'teatree1').map((x) => x.sku), ['teatree10'], '商品ID部分一致');
  eq(searchProducts(db, '%').length, 0, 'LIKEメタ文字はエスケープ (%で全件ヒットしない)');
  eq(searchProducts(db, '  ').length, 0, '空白のみは0件');
}

console.log('\n── handleChatEvent: MESSAGE ──');
{
  const noHit = handleChatEvent({ type: 'MESSAGE', message: { text: '存在しない商品XYZ' } }, db, NOW);
  ok(noHit.text.includes('見つかりませんでした'), `0件: ${noHit.text}`);

  const single = handleChatEvent({ type: 'MESSAGE', message: { argumentText: ' 10ml ' } }, db, NOW);
  ok(single.text.includes('teatree10') && single.text.includes('在庫ロケーション'), `1件は即ロケ表示:\n${single.text}`);

  const multi = handleChatEvent({ type: 'MESSAGE', message: { text: 'ティーツリー' } }, db, NOW);
  const buttons = multi.cardsV2[0].card.sections[0].widgets[0].buttonList.buttons;
  eq(buttons.length, 2, '2件はボタン2つ');
  ok(buttons[0].text.includes('20ml') && buttons[0].text.includes('フリー245'), `ボタン表記: ${buttons[0].text}`);
  eq(buttons[0].onClick.action.function, 'showStock', 'タップで showStock');
  eq(buttons[0].onClick.action.parameters, [{ key: 'sku', value: 'teatree20' }], 'パラメータにSKU');

  const many = handleChatEvent({ type: 'MESSAGE', message: { text: 'ハッカ油テスト' } }, db, NOW);
  const manyButtons = many.cardsV2[0].card.sections[0].widgets[0].buttonList.buttons;
  eq(manyButtons.length, 10, '11件以上でも上限10ボタン');
  const widgets = many.cardsV2[0].card.sections[0].widgets;
  ok(JSON.stringify(widgets).includes('絞って'), '上限超過は絞り込み案内つき');

  const empty = handleChatEvent({ type: 'MESSAGE', message: { text: '' } }, db, NOW);
  ok(empty.text.includes('商品名'), '空メッセージは使い方案内');
}

console.log('\n── handleChatEvent: CARD_CLICKED ──');
{
  const newFmt = handleChatEvent({ type: 'CARD_CLICKED', common: { invokedFunction: 'showStock', parameters: { sku: 'teatree20' } } }, db, NOW);
  eq(newFmt.actionResponse, { type: 'NEW_MESSAGE' }, '新形式: NEW_MESSAGE 応答');
  const lines = newFmt.text.split('\n');
  ok(lines[0] === 'ティーツリーオイル 20ml' && lines[1] === '(teatree20)', `商品名+SKU見出し:\n${newFmt.text}`);
  ok(newFmt.text.includes('📍 在庫ロケーション (16時00分時点)'), '取得時刻つき見出し (コロン不使用=LINE/GChat共通整形)');
  ok(newFmt.text.includes('・R1FA-002-001-01 → 200個') && newFmt.text.includes('・P1FB-001-002-01 → 30個'), 'フリー降順のロケ行');
  ok(newFmt.text.includes('・ZZZ-ZZZ-ZZ → 15個'), '特殊ロケZZZは実名のまま末尾に表示');
  ok(!newFmt.text.includes('P2FA'), '不良品ロケは出ない');

  const oldFmt = handleChatEvent({ type: 'CARD_CLICKED', action: { actionMethodName: 'showStock', parameters: [{ key: 'sku', value: 'teatree10' }] } }, db, NOW);
  ok(oldFmt.text.includes('(teatree10)') && oldFmt.text.includes('・R1FA-001-001-01 → 40個 (期限2028/01/15・別途引当10)'), `旧形式パラメータでも動く+期限表示:\n${oldFmt.text}`);

  const gone = handleChatEvent({ type: 'CARD_CLICKED', common: { invokedFunction: 'showStock', parameters: { sku: 'nolongerexists' } } }, db, NOW);
  ok(gone.text.includes('見つかりませんでした'), 'SKU消失は案内文');

  const badFn = handleChatEvent({ type: 'CARD_CLICKED', common: { invokedFunction: 'other' } }, db, NOW);
  ok(badFn.text.includes('もう一度'), '未知のボタンはエラー案内');
}

console.log('\n── handleChatEvent: その他イベント ──');
{
  const added = handleChatEvent({ type: 'ADDED_TO_SPACE' }, db, NOW);
  ok(added.text.includes('在庫検索ボット'), 'スペース追加で挨拶+使い方');
  eq(handleChatEvent({ type: 'REMOVED_FROM_SPACE' }, db, NOW), {}, '未対応イベントは空応答');
}

console.log('\n── buildStockReply ──');
{
  eq(buildStockReply(db, 'zzz-nothing', NOW), null, '存在しないSKUは null');
  ok(buildStockReply(db, ' TEATREE20 ', NOW)?.includes('(teatree20)') ?? false, 'SKUはtrim+小文字で照会');
}

console.log('\n── 1文字検索とレート制限 ──');
{
  const short = handleChatEvent({ type: 'MESSAGE', message: { text: '油' } }, db, NOW);
  ok(short.text.includes('2文字以上'), '1文字は案内を返す (全表走査の無駄撃ち防止)');

  const t0 = 1_000_000;
  let first20 = true;
  for (let i = 0; i < 20; i++) first20 = first20 && checkRateLimit('space-A', t0 + i * 100);
  ok(first20, '同一スペース20回/分までは許可');
  ok(!checkRateLimit('space-A', t0 + 5_000), '同一スペース21回目/分は制限');
  ok(checkRateLimit('space-B', t0 + 5_000), '別スペースは独立');
  ok(checkRateLimit('space-A', t0 + 61_000), '1分経過でリセット');
}

console.log('\n── HTTPレベル (認証がparserより前・ドメイン制限・413) ──');
{
  const { default: express } = await import('express');
  const http = await import('node:http');
  const app = express();
  const auth = makeStockBotAuth(async (a) => a === 'Bearer good-token');
  app.use('/apps/stock-bot', auth, express.json({ limit: '256kb' }), stockBotRouter);
  // 413 (PayloadTooLarge) を finalhandler に流すと Node 24 では非同期の再throwで
  // プロセスが exit 255 になる (テストの後始末事故)。明示ハンドラで応答して閉じる
  // eslint-disable-next-line no-unused-vars
  app.use((err, req, res, next) => { res.status(err.status || 500).json({ error: err.type || 'error' }); });
  const server = http.createServer(app);
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${server.address().port}`;
  const post = (body, headers = {}) => fetch(`${base}/apps/stock-bot/chat-events`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: typeof body === 'string' ? body : JSON.stringify(body),
  });

  eq((await post({ type: 'MESSAGE' })).status, 401, 'Bearerなしは401');
  eq((await post({ type: 'MESSAGE' }, { Authorization: 'Bearer bad' })).status, 401, '検証失敗は401');

  const outsider = await post(
    { type: 'MESSAGE', user: { email: 'x@gmail.com' }, message: { text: 'ティーツリー' } },
    { Authorization: 'Bearer good-token' });
  ok((await outsider.json()).text.includes('社内ユーザー'), '社外ドメインは拒否');

  const noEmail = await post(
    { type: 'MESSAGE', message: { text: 'ティーツリー' } },
    { Authorization: 'Bearer good-token' });
  ok((await noEmail.json()).text.includes('利用者を確認できなかった'), 'メール欠落のMESSAGEはfail-closed');

  const insider = await post(
    { type: 'MESSAGE', user: { email: 'd.nakahara@b-faith.biz' }, space: { name: 'spaces/test' }, message: { text: 'ティーツリー' } },
    { Authorization: 'Bearer good-token' });
  const insiderBody = await insider.json();
  ok(Array.isArray(insiderBody.cardsV2), '社内ユーザーの検索は候補カードが返る');

  const big = await post(
    `{"type":"MESSAGE","pad":"${'x'.repeat(300 * 1024)}"}`,
    { Authorization: 'Bearer good-token' });
  eq(big.status, 413, '256KB超のbodyは413');

  await new Promise((r) => server.close(r));
}

db.close();
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* 後始末失敗は無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
