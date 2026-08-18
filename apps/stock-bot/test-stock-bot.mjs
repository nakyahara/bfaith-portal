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
const { searchProducts, buildStockReply, handleChatEvent, makeStockBotAuth, checkRateLimit,
  adaptChatEvent, wrapAddonResponse, userEmailFromIdToken } = routerMod;
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
  eq(buttons[0].onClick.action.parameters,
    [{ key: 'method', value: 'showStock' }, { key: 'sku', value: 'teatree20' }],
    'パラメータに method 併記+SKU (アドオン形式で function 名が届かない対策)');

  const many = handleChatEvent({ type: 'MESSAGE', message: { text: 'ハッカ油テスト' } }, db, NOW);
  const manyButtons = many.cardsV2[0].card.sections[0].widgets[0].buttonList.buttons;
  eq(manyButtons.length, 10, '11件以上でも上限10ボタン');
  const widgets = many.cardsV2[0].card.sections[0].widgets;
  ok(JSON.stringify(widgets).includes('絞って'), '上限超過は絞り込み案内つき');

  const empty = handleChatEvent({ type: 'MESSAGE', message: { text: '' } }, db, NOW);
  ok(empty.text.includes('商品名'), '空メッセージは使い方案内');

  // アドオン形式: argumentText 無し+@メンション入り本文 → annotations の表示名で除去
  const mentioned = handleChatEvent({ type: 'MESSAGE', message: {
    text: '@在庫検索ボット ティーツリー',
    annotations: [{ type: 'USER_MENTION', userMention: { user: { displayName: '在庫検索ボット' } } }],
  } }, db, NOW);
  ok(Array.isArray(mentioned.cardsV2), 'メンション除去して検索 (annotations経由)');

  // annotations 無しでも先頭@トークンの保険で動く
  const mentionedNoAnn = handleChatEvent({ type: 'MESSAGE', message: { text: '@在庫検索ボット ティーツリー' } }, db, NOW);
  ok(Array.isArray(mentionedNoAnn.cardsV2), 'メンション除去して検索 (保険の正規表現)');

  // メンションのみは使い方案内
  const mentionOnly = handleChatEvent({ type: 'MESSAGE', message: {
    text: '@在庫検索ボット',
    annotations: [{ type: 'USER_MENTION', userMention: { user: { displayName: '在庫検索ボット' } } }],
  } }, db, NOW);
  ok(mentionOnly.text.includes('商品名'), 'メンションのみは使い方案内');

  // ボット名と同じ文字列が検索語に含まれても消えない (1注釈=1除去)
  const sameName = handleChatEvent({ type: 'MESSAGE', message: {
    text: '@ティーツリー ティーツリー',
    annotations: [{ type: 'USER_MENTION', userMention: { user: { displayName: 'ティーツリー' } } }],
  } }, db, NOW);
  ok(Array.isArray(sameName.cardsV2), 'ボット名=検索語でも検索語は残る');

  // argumentText がある旧形式はそちらを優先 (@付き本文でも影響なし)
  const withArg = handleChatEvent({ type: 'MESSAGE', message: { text: '@bot 10ml', argumentText: ' 10ml ' } }, db, NOW);
  ok(withArg.text.includes('teatree10'), 'argumentText 優先は従来どおり');
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

  // アドオン形式: invokedFunction が URL で届いても method パラメータで解決できる
  const byMethod = handleChatEvent({
    type: 'CARD_CLICKED',
    common: { invokedFunction: 'https://example.com/endpoint', parameters: { method: 'showStock', sku: 'teatree20' } },
  }, db, NOW);
  ok(byMethod.text.includes('(teatree20)'), 'method パラメータ優先で showStock 解決');

  // アドオン移行時の旧 function 名の受け皿 (__action_method_name__)
  const byLegacyName = handleChatEvent({
    type: 'CARD_CLICKED',
    common: { parameters: { __action_method_name__: 'showStock', sku: 'teatree10' } },
  }, db, NOW);
  ok(byLegacyName.text.includes('(teatree10)'), '__action_method_name__ でも解決');
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

console.log('\n── Workspaceアドオン形式の互換 (adaptChatEvent / wrapAddonResponse) ──');
{
  // MESSAGE: chat.messagePayload → 旧形式 MESSAGE (email 無しはそのまま = 補完はルート側で署名検証つき)
  const msg = adaptChatEvent({
    chat: {
      user: { name: 'users/1', displayName: '中原', type: 'HUMAN' },
      messagePayload: {
        message: { text: '@在庫検索ボット ひのき', argumentText: ' ひのき' },
        space: { name: 'spaces/AAQAoXoR0f4' },
      },
    },
  });
  ok(msg.addon, 'アドオン形式を検出');
  eq(msg.event.type, 'MESSAGE', 'messagePayload → MESSAGE');
  eq(msg.event.user.email, undefined, 'email 無しは補完しない (未検証decodeはしない)');
  eq(msg.event.space.name, 'spaces/AAQAoXoR0f4', 'space を引き上げ');
  eq(msg.event.message.argumentText, ' ひのき', 'message はそのまま');

  // chat.user の email はそのまま通る
  const withEmail = adaptChatEvent({
    chat: { user: { email: 'x@b-faith.biz' }, messagePayload: { message: { text: 'a' }, space: { name: 'spaces/x' } } },
  });
  eq(withEmail.event.user.email, 'x@b-faith.biz', 'chat.user.email はそのまま');

  // userIdToken の email は署名検証済みのみ採用 (fail-closed)
  const b64u = (o) => Buffer.from(JSON.stringify(o)).toString('base64url');
  const fakeToken = `${b64u({ alg: 'RS256' })}.${b64u({ email: 'x@b-faith.biz', email_verified: true })}.sig`;
  eq(await userEmailFromIdToken(fakeToken), '', '署名検証に通らないトークンは email 空 (fail-closed)');
  eq(await userEmailFromIdToken(''), '', 'トークン無しは空');
  const okClient = { verifyIdToken: async () => ({ getPayload: () => ({ email: 'y@b-faith.biz', email_verified: true }) }) };
  eq(await userEmailFromIdToken('tok', okClient), 'y@b-faith.biz', '検証済みトークンの email を採用');
  const unverified = { verifyIdToken: async () => ({ getPayload: () => ({ email: 'y@b-faith.biz', email_verified: false }) }) };
  eq(await userEmailFromIdToken('tok', unverified), '', 'email_verified=false は不採用');

  // CARD_CLICKED: buttonClickedPayload + commonEventObject (parameters は map 形式)
  const click = adaptChatEvent({
    chat: {
      user: { email: 'd.nakahara@b-faith.biz' },
      buttonClickedPayload: { message: { space: { name: 'spaces/AAQAoXoR0f4' } } },
    },
    commonEventObject: { invokedFunction: 'showStock', parameters: { sku: 'teatree20' } },
  });
  eq(click.event.type, 'CARD_CLICKED', 'buttonClickedPayload → CARD_CLICKED');
  eq(click.event.common.invokedFunction, 'showStock', 'invokedFunction を common へ');
  eq(click.event.common.parameters, { sku: 'teatree20' }, 'parameters map をそのまま');
  eq(click.event.space.name, 'spaces/AAQAoXoR0f4', 'space は message.space からも補完');
  const clickOut = handleChatEvent(click.event, db, NOW);
  ok(clickOut.text.includes('(teatree20)'), '変換後イベントで既存ロジックが動く');

  // ADDED_TO_SPACE / appCommand / 未知payload
  eq(adaptChatEvent({ chat: { addedToSpacePayload: { space: { name: 'spaces/x' } } } }).event.type,
    'ADDED_TO_SPACE', 'addedToSpacePayload → ADDED_TO_SPACE');
  eq(adaptChatEvent({ chat: { appCommandPayload: {} } }).event.type, undefined, '本文なし appCommand は type 無し (空応答へ)');
  eq(adaptChatEvent({
    chat: { appCommandPayload: { message: { text: '/zaiko ひのき' }, space: { name: 'spaces/x' } } },
    commonEventObject: { invokedFunction: 'https://example.com/x' },
  }).event.type, 'MESSAGE', 'スラッシュコマンドは MESSAGE (invokedFunction 同居でも CARD_CLICKED に誤分類しない)');

  // 旧形式は素通し
  const legacy = adaptChatEvent({ type: 'MESSAGE', message: { text: 'a' } });
  ok(!legacy.addon && legacy.event.type === 'MESSAGE', '旧形式はそのまま');

  // parameters のみ届くクリック (invokedFunction・buttonClickedPayload 無し) も CARD_CLICKED
  const paramOnlyClick = adaptChatEvent({
    chat: { user: { email: 'd.nakahara@b-faith.biz' } },
    commonEventObject: { parameters: { method: 'showStock', sku: 'teatree20' } },
  });
  eq(paramOnlyClick.event.type, 'CARD_CLICKED', 'parameters.method だけでもクリックと判定');
  ok(handleChatEvent(paramOnlyClick.event, db, NOW).text.includes('(teatree20)'), 'そのまま在庫応答まで通る');

  // wrapAddonResponse
  eq(wrapAddonResponse({}), {}, '空応答は空のまま');
  eq(wrapAddonResponse({ text: 'こんにちは' }),
    { hostAppDataAction: { chatDataAction: { createMessageAction: { message: { text: 'こんにちは' } } } } },
    'テキストは createMessageAction に包む');
  const upd = wrapAddonResponse({ actionResponse: { type: 'UPDATE_MESSAGE' }, text: '✅承認済み' });
  ok(!!upd.hostAppDataAction.chatDataAction.updateMessageAction, 'UPDATE_MESSAGE は updateMessageAction');
  const card = wrapAddonResponse({ text: '候補', cardsV2: [{ cardId: 'x', card: {} }] });
  ok(Array.isArray(card.hostAppDataAction.chatDataAction.createMessageAction.message.cardsV2), 'cardsV2 も message に載る');

  // カード内ボタンの function 名 → エンドポイントURLへ差し替え (アドオン形式の必須要件)
  const btnCard = wrapAddonResponse({
    text: '候補',
    cardsV2: [{ cardId: 'x', card: { sections: [{ widgets: [{ buttonList: { buttons: [
      { text: 'A', onClick: { action: { function: 'showStock', parameters: [{ key: 'method', value: 'showStock' }, { key: 'sku', value: 'teatree20' }] } } },
      { text: 'B', onClick: { action: { function: 'noMethodParam', parameters: [{ key: 'id', value: '1' }] } } },
    ] } }] }] } }],
  });
  const outButtons = btnCard.hostAppDataAction.chatDataAction.createMessageAction.message
    .cardsV2[0].card.sections[0].widgets[0].buttonList.buttons;
  ok(outButtons[0].onClick.action.function.startsWith('https://'), 'function は URL に差し替え');
  eq(outButtons[0].onClick.action.parameters.filter((p) => p.key === 'method').length, 1, 'method 併記済みは重複追加しない');
  eq(outButtons[1].onClick.action.parameters.find((p) => p.key === 'method')?.value, 'noMethodParam',
    'method 未併記のボタンは function 名を method へ退避');
  ok(outButtons[1].onClick.action.function.startsWith('https://'), '未併記側も URL に差し替え');

  // 既存 method が function と食い違う場合は function 名で上書き (取り違え防止)
  const mismatch = wrapAddonResponse({
    cardsV2: [{ card: { sections: [{ widgets: [{ buttonList: { buttons: [
      { text: 'C', onClick: { action: { function: 'realFn', parameters: [{ key: 'method', value: 'other' }] } } },
    ] } }] }] } }],
    text: 'x',
  });
  eq(mismatch.hostAppDataAction.chatDataAction.createMessageAction.message
    .cardsV2[0].card.sections[0].widgets[0].buttonList.buttons[0].onClick.action.parameters
    .find((p) => p.key === 'method')?.value, 'realFn', 'method 不一致は function 名で上書き');
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

  // アドオン形式: 応答が hostAppDataAction に包まれる
  const addonSearch = await post({
    chat: {
      user: { email: 'd.nakahara@b-faith.biz' },
      messagePayload: { message: { text: 'ティーツリー' }, space: { name: 'spaces/addon-test' } },
    },
  }, { Authorization: 'Bearer good-token' });
  const addonBody = await addonSearch.json();
  const addonMsg = addonBody.hostAppDataAction?.chatDataAction?.createMessageAction?.message;
  ok(Array.isArray(addonMsg?.cardsV2), 'アドオン形式の検索は createMessageAction で候補カードが返る');

  const addonOutsider = await post({
    chat: {
      user: { email: 'x@gmail.com' },
      messagePayload: { message: { text: 'ティーツリー' }, space: { name: 'spaces/addon-test' } },
    },
  }, { Authorization: 'Bearer good-token' });
  const outText = (await addonOutsider.json()).hostAppDataAction?.chatDataAction?.createMessageAction?.message?.text;
  ok(String(outText).includes('社内ユーザー'), 'アドオン形式でも社外ドメイン拒否 (応答も包まれる)');

  // email 無し + 検証に通らない userIdToken → fail-closed の案内 (アドオン形式で包まれる)
  const addonNoEmail = await post({
    chat: { user: { name: 'users/9' }, messagePayload: { message: { text: 'ティーツリー' }, space: { name: 'spaces/addon-test' } } },
    authorizationEventObject: { userIdToken: 'garbage.token.here' },
  }, { Authorization: 'Bearer good-token' });
  const noEmailText = (await addonNoEmail.json()).hostAppDataAction?.chatDataAction?.createMessageAction?.message?.text;
  ok(String(noEmailText).includes('利用者を確認できなかった'), '未検証 userIdToken は email 扱いしない (fail-closed)');

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
