/**
 * picking — LINE在庫検索ボット (line-search.js) のテスト。
 * warehouse呼び出しとLINE reply APIは deps 注入で差し替え、実APIは叩かない。
 */
import assert from 'node:assert/strict';

delete process.env.PICKING_LINE_SEARCH_TO;
delete process.env.PICKING_LINE_CHANNEL_TOKEN;

const {
  isSearchSource, buildSearchReplyMessages, buildPostbackReplyMessages, processLineEvents, reserveEvent,
  resolveBlockCommand, buildBlockListMessages, chunkTextMessages,
} = await import('../line-search.js');

let passed = 0;
const t = (name, fn) => Promise.resolve(fn()).then(() => { passed++; console.log(`  ok: ${name}`); });

const FRESH = new Date().toISOString();
const ITEM = (sku, name, free) => ({ sku, name, free });
const LOCATIONS = {
  ok: true, importedAt: FRESH, stockDate: '20260816', name: 'ティーツリーオイル 20ml',
  locations: [
    { block: 'R1FA', location: '002-001-01', quality: '良品', qty: 200, allocated: 0, free: 200, expiry: '20280115' },
    { block: 'ZZZ', location: 'ZZZ-ZZZ-ZZ', quality: '良品', qty: 15, allocated: 0, free: 15 },
  ],
};
const deps = (over = {}) => ({
  fetchStockSearch: async () => ({ ok: true, importedAt: FRESH, items: [ITEM('teatree20', 'ティーツリーオイル 20ml', 245), ITEM('teatree10', 'ティーツリーオイル 10ml', 40)] }),
  fetchStockLocations: async () => LOCATIONS,
  // 既定は「どのブロックも在庫なし」= ブロック名直打ちは商品検索へフォールバックする
  fetchStockBlock: async (code) => ({ ok: true, importedAt: FRESH, block: code, items: [] }),
  reply: async () => true,
  ...over,
});

// ─── isSearchSource (応答してよい場所の判定) ───
await t('isSearchSource: 1:1は常にOK・グループは許可リストのみ', () => {
  assert.equal(isSearchSource({ type: 'user', userId: 'U1' }), true);
  assert.equal(isSearchSource({ type: 'group', groupId: 'Cgrp1' }), false, '未設定なら全グループ拒否');
  process.env.PICKING_LINE_SEARCH_TO = 'Cgrp1, Rroom1';
  assert.equal(isSearchSource({ type: 'group', groupId: 'Cgrp1' }), true);
  assert.equal(isSearchSource({ type: 'group', groupId: 'Cketsupin' }), false, '欠品通知グループには反応しない');
  assert.equal(isSearchSource({ type: 'room', roomId: 'Rroom1' }), true);
  assert.equal(isSearchSource(undefined), false);
});

// ─── buildSearchReplyMessages ───
await t('検索: 空/1文字は案内', async () => {
  assert.ok((await buildSearchReplyMessages('', deps()))[0].text.includes('商品名'));
  assert.ok((await buildSearchReplyMessages('油', deps()))[0].text.includes('2文字以上'));
});

await t('検索: warehouse停止はエラー案内 (無反応にしない)', async () => {
  const msgs = await buildSearchReplyMessages('ティーツリー', deps({ fetchStockSearch: async () => null }));
  assert.ok(msgs[0].text.includes('取得できませんでした'), msgs[0].text);
});

await t('検索: 0件は見つからない案内', async () => {
  const msgs = await buildSearchReplyMessages('ない商品', deps({ fetchStockSearch: async () => ({ ok: true, importedAt: FRESH, items: [] }) }));
  assert.ok(msgs[0].text.includes('見つかりませんでした'));
});

await t('検索: 1件は即ロケ表示 (仮想ロケ合算つき)', async () => {
  const msgs = await buildSearchReplyMessages('20ml', deps({ fetchStockSearch: async () => ({ ok: true, importedAt: FRESH, items: [ITEM('teatree20', 'ティーツリーオイル 20ml', 245)] }) }));
  const text = msgs[0].text;
  assert.ok(text.startsWith('ティーツリーオイル 20ml\n(teatree20)'), text);
  assert.ok(text.includes('📍 在庫ロケーション') && text.includes('・R1FA-002-001-01 → 200個 (期限2028/01/15)'), text);
  assert.ok(text.includes('・ZZZ-ZZZ-ZZ → 15個'), text);
  assert.ok(!/\d: ?\d/.test(text), '「数字:数字」を含まない (LINEの時刻リンク化対策)');
  assert.equal(msgs[0].quickReply, undefined, '確定表示にボタンは付けない');
});

await t('検索: 複数件は番号リスト+クイックリプライ (postback)', async () => {
  const msgs = await buildSearchReplyMessages('ティーツリー', deps());
  const m = msgs[0];
  assert.ok(m.text.includes('① ティーツリーオイル 20ml — フリー245'), m.text);
  assert.ok(m.text.includes('② ティーツリーオイル 10ml — フリー40'), m.text);
  const items = m.quickReply.items;
  assert.equal(items.length, 2);
  assert.equal(items[0].action.type, 'postback');
  assert.equal(items[0].action.data, 'stock:teatree20');
  assert.ok(items[0].action.label.length <= 20, `ラベル20文字制限: "${items[0].action.label}"`);
  assert.ok(items[0].action.label.startsWith('①'));
});

await t('検索: 11件以上は10件+絞り込み案内・長い商品名はラベル切り詰め', async () => {
  const many = Array.from({ length: 11 }, (_, i) => ITEM(`sku${i}`, `とても長い商品名のテスト用データ${i}番目です`, 100 - i));
  const msgs = await buildSearchReplyMessages('テスト', deps({ fetchStockSearch: async () => ({ ok: true, importedAt: FRESH, items: many }) }));
  const m = msgs[0];
  assert.equal(m.quickReply.items.length, 10, 'ボタンは10個まで');
  assert.ok(m.text.includes('絞って'), m.text);
  for (const it of m.quickReply.items) assert.ok(it.action.label.length <= 20, `label="${it.action.label}"`);
});

// ─── buildPostbackReplyMessages ───
await t('postback: stock:sku はロケ表示・他のdataは無視 (null)', async () => {
  const msgs = await buildPostbackReplyMessages('stock:teatree20', deps());
  assert.ok(msgs[0].text.includes('(teatree20)') && msgs[0].text.includes('在庫ロケーション'));
  assert.equal(await buildPostbackReplyMessages('other:xxx', deps()), null);
  assert.equal(await buildPostbackReplyMessages('', deps()), null);
  const gone = await buildPostbackReplyMessages('stock:vanished', deps({ fetchStockLocations: async () => null }));
  assert.ok(gone[0].text.includes('見つかりませんでした'));
});

await t('postback: 不正なdata (長すぎ/制御文字/空) は無視', async () => {
  assert.equal(await buildPostbackReplyMessages(`stock:${'x'.repeat(201)}`, deps()), null);
  assert.equal(await buildPostbackReplyMessages('stock:abc\u0000def', deps()), null);
  assert.equal(await buildPostbackReplyMessages('stock:   ', deps()), null);
});

await t('検索: 異常に長いSKUは件数分岐の前に除外 (残り1件なら即ロケ表示)', async () => {
  const msgs = await buildSearchReplyMessages('テスト', deps({
    fetchStockSearch: async () => ({ ok: true, importedAt: FRESH, items: [ITEM('x'.repeat(250), '変なSKU', 5), ITEM('okone', '正常な商品', 3)] }),
  }));
  assert.ok(msgs[0].text.includes('(okone)') && msgs[0].text.includes('在庫ロケーション'), msgs[0].text);
  assert.equal(msgs[0].quickReply, undefined);
  // 長大SKUしか無ければ「見つからない」扱い (locations照会にも本文にも載せない)
  const onlyBad = await buildSearchReplyMessages('テスト', deps({
    fetchStockSearch: async () => ({ ok: true, importedAt: FRESH, items: [ITEM('x'.repeat(250), '変なSKU', 5)] }),
  }));
  assert.ok(onlyBad[0].text.includes('見つかりませんでした'), onlyBad[0].text);
});

await t('検索: 100文字超のキーワードは拒否・0件応答は入力を30文字に切り詰め', async () => {
  const long = await buildSearchReplyMessages('あ'.repeat(101), deps());
  assert.ok(long[0].text.includes('長すぎます'), long[0].text);
  const echo = await buildSearchReplyMessages('か'.repeat(100), deps({ fetchStockSearch: async () => ({ ok: true, importedAt: FRESH, items: [] }) }));
  assert.ok(!echo[0].text.includes('か'.repeat(31)), '0件応答に長い入力を全文エコーしない');
});

// ─── ブロック一覧コマンド (Z/A) ───
const BLOCK_ROW = (o = {}) => ({
  location: 'ZZZ-ZZZ-ZZ', sku: 'hakka200', name: 'ハッカ油 【200ml】 _パフ箱', quality: '良品',
  expiry: '20280115', qty: 2600, allocated: 0, free: 2600, ...o,
});

await t('resolveBlockCommand: Z/A エイリアス・全角・ロケ付き・直打ち・非該当', () => {
  assert.deepEqual(resolveBlockCommand('Z'), { block: 'ZZZ', explicit: true });
  assert.deepEqual(resolveBlockCommand('z'), { block: 'ZZZ', explicit: true });
  assert.deepEqual(resolveBlockCommand('Ｚ'), { block: 'ZZZ', explicit: true }, '全角も同一視');
  assert.deepEqual(resolveBlockCommand('Zロケ'), { block: 'ZZZ', explicit: true });
  assert.deepEqual(resolveBlockCommand('A'), { block: 'AAAA', explicit: true });
  assert.deepEqual(resolveBlockCommand('Y'), { block: 'YYY', explicit: true }, 'YもYYYのエイリアス');
  assert.deepEqual(resolveBlockCommand('YYY'), { block: 'YYY', explicit: false }, 'ブロック名直打ち');
  assert.equal(resolveBlockCommand('ハッカ'), null);
  assert.equal(resolveBlockCommand('teatree10'), null, '7文字以上は商品検索');
});

await t('ブロック一覧: 単一ロケはロケ省略・フリー降順・期限/品質/引当の補足・ヘッダ集計', async () => {
  const d = deps({
    fetchStockBlock: async (code) => ({
      ok: true, importedAt: FRESH, block: code,
      items: [
        BLOCK_ROW({ sku: 'amaniyu100', name: '木工用 亜麻仁油 【100ml 】', quality: 'Ｂ品', expiry: '', qty: 6, free: 6 }),
        BLOCK_ROW(),
        BLOCK_ROW({ sku: 'byobin', name: '上敷鋲 【25本入り】', expiry: '', qty: 460, free: 460, allocated: 10 }),
        BLOCK_ROW({ sku: 'zero', name: 'フリー0は出さない', free: 0 }),
      ],
    }),
  });
  const msgs = await buildSearchReplyMessages('Z', d);
  assert.equal(msgs.length, 1);
  const lines = msgs[0].text.split('\n');
  assert.ok(lines[0].startsWith('📦 ZZZ の在庫一覧 ('), lines[0]);
  assert.equal(lines[1], '3件・フリー計3,066個');
  assert.equal(lines[2], '', 'ヘッダ後に空行');
  // 数量が行頭 (行末だと商品名の折り返しで迷子)・千位カンマ・社内サフィックス (_パフ箱等) は省く
  assert.equal(lines[3], '・2,600個｜ハッカ油 【200ml】 (期限2028/01/15)');
  assert.equal(lines[4], '・460個｜上敷鋲 【25本入り】 (別途引当10)');
  assert.equal(lines[5], '・6個｜木工用 亜麻仁油 【100ml 】 (Ｂ品)');
  assert.equal(lines.length, 6, '単一ロケなので [ロケ] プレフィックスなし');
});

await t('ブロック一覧: サフィックス省略は社内タグ限定 (正当な _Type-C 等は残す)', async () => {
  const d = deps({
    fetchStockBlock: async () => ({
      ok: true, importedAt: FRESH, block: 'ZZZ',
      items: [
        BLOCK_ROW({ name: 'ハッカ油 【500ml】_K-44', expiry: '', free: 400, qty: 400 }),
        BLOCK_ROW({ sku: 'cable1', name: '充電ケーブル_Type-C', expiry: '', free: 30, qty: 30 }),
        BLOCK_ROW({ sku: 'vitamin', name: 'ビタミン_C', expiry: '', free: 20, qty: 20 }),
      ],
    }),
  });
  const text = (await buildSearchReplyMessages('Z', d))[0].text;
  assert.ok(text.includes('・400個｜ハッカ油 【500ml】'), `_K-44は省く: ${text}`);
  assert.ok(!text.includes('_K-44'), text);
  assert.ok(text.includes('充電ケーブル_Type-C'), '_Type-C は正当な名前として残す');
  assert.ok(text.includes('ビタミン_C'), '_C も残す');
});

await t('ブロック一覧: 複数ロケは [ロケ] プレフィックス付き', async () => {
  const d = deps({
    fetchStockBlock: async () => ({
      ok: true, importedAt: FRESH, block: 'AAAA',
      items: [
        BLOCK_ROW({ location: '001-001-01', name: '商品X', expiry: '' }),
        BLOCK_ROW({ location: '001-002-01', name: '商品Y', expiry: '', free: 10, qty: 10 }),
      ],
    }),
  });
  const msgs = await buildSearchReplyMessages('A', d);
  assert.ok(msgs[0].text.includes('個｜[001-001-01] '), msgs[0].text);
  assert.ok(msgs[0].text.includes('・10個｜[001-002-01] 商品Y'), msgs[0].text);
});

await t('ブロック一覧: エイリアス在庫0=在庫なし案内・直打ちは商品検索優先・商品0件でブロック照会', async () => {
  const d = deps({ fetchStockBlock: async () => ({ ok: true, importedAt: FRESH, block: 'AAAA', items: [] }) });
  const empty = await buildSearchReplyMessages('A', d);
  assert.ok(empty[0].text.includes('AAAA に現在在庫はありません'), empty[0].text);
  // 直打ち 'YYY' は商品検索が優先 (deps既定のfetchStockSearchが2件返す → 候補ボタン。block照会しない)
  let blockCalled = false;
  const d2 = deps({ fetchStockBlock: async () => { blockCalled = true; return { ok: true, importedAt: FRESH, block: 'YYY', items: [BLOCK_ROW()] }; } });
  const productFirst = await buildSearchReplyMessages('YYY', d2);
  assert.ok(productFirst[0].quickReply, `商品検索が優先されるはず: ${productFirst[0].text}`);
  assert.equal(blockCalled, false, '商品ヒット時はブロック照会しない (通常検索に遅延を足さない)');
  // 商品0件 → ブロック照会にフォールバックして一覧を返す
  const d3 = deps({
    fetchStockSearch: async () => ({ ok: true, importedAt: FRESH, items: [] }),
    fetchStockBlock: async () => ({ ok: true, importedAt: FRESH, block: 'YYY', items: [BLOCK_ROW()] }),
  });
  const blockHit = await buildSearchReplyMessages('YYY', d3);
  assert.ok(blockHit[0].text.startsWith('📦 YYY の在庫一覧'), blockHit[0].text);
  // エイリアスで取得失敗 → エラー案内
  const fail = await buildSearchReplyMessages('Z', deps({ fetchStockBlock: async () => null }));
  assert.ok(fail[0].text.includes('取得できませんでした'), fail[0].text);
});

await t('chunkTextMessages: 5,000字対策の分割と5通上限', () => {
  const lines = Array.from({ length: 600 }, (_, i) => `・行${i} ${'あ'.repeat(50)}`);
  const msgs = chunkTextMessages('HEADER', lines);
  assert.ok(msgs.length <= 5, `最大5通: ${msgs.length}`);
  for (const m of msgs) assert.ok(m.text.length <= 5000, `1通5,000字以内: ${m.text.length}`);
  assert.ok(msgs[msgs.length - 1].text.includes('省略'), '溢れは明示');
  const small = chunkTextMessages('H', ['・a', '・b']);
  assert.equal(small.length, 1);
  assert.equal(small[0].text, 'H\n・a\n・b');
  // 1行だけで上限超の異常データは行内で切り詰める (1通5,000字超で返信ごと拒否されない)
  const longLine = chunkTextMessages('H', [`・${'x'.repeat(6000)}`]);
  for (const m of longLine) assert.ok(m.text.length <= 5000, `長い1行も上限内: ${m.text.length}`);
  assert.ok(longLine.some((m) => m.text.includes('…')), '切り詰めを明示');
});

// ─── processLineEvents (e2e・応答場所の制御) ───
await t('processLineEvents: 1:1と許可グループだけ返信・欠品通知グループは沈黙', async () => {
  const sent = [];
  const d = deps({ reply: async (token, messages) => { sent.push({ token, messages }); return true; } });
  process.env.PICKING_LINE_SEARCH_TO = 'Csearch';
  await processLineEvents([
    { type: 'message', replyToken: 'r1', source: { type: 'user', userId: 'U1' }, message: { type: 'text', text: 'ティーツリー' } },
    { type: 'message', replyToken: 'r2', source: { type: 'group', groupId: 'Cketsupin' }, message: { type: 'text', text: '欠品対応しました' } },
    { type: 'message', replyToken: 'r3', source: { type: 'group', groupId: 'Csearch' }, message: { type: 'text', text: '20ml' } },
    { type: 'postback', replyToken: 'r4', source: { type: 'user', userId: 'U1' }, postback: { data: 'stock:teatree20' } },
    { type: 'message', source: { type: 'user', userId: 'U1' }, message: { type: 'text', text: 'tokenなし' } },
    { type: 'message', replyToken: 'r6', source: { type: 'user', userId: 'U1' }, message: { type: 'image' } },
  ], d);
  // ワーカープール並行処理のため完了順は不定 → 順不同で比較
  assert.deepEqual(sent.map((s) => s.token).sort(), ['r1', 'r3', 'r4'], '返信は 1:1検索・許可グループ・postback の3件だけ');
});

await t('processLineEvents: webhook再送 (同一webhookEventId) は1回だけ処理', async () => {
  const sent = [];
  const d = deps({ reply: async (token) => { sent.push(token); return true; } });
  const ev = { type: 'postback', replyToken: 'rd1', webhookEventId: 'W-dup-1', source: { type: 'user', userId: 'U1' }, postback: { data: 'stock:teatree20' } };
  await processLineEvents([ev], d);
  await processLineEvents([{ ...ev, replyToken: 'rd2' }], d);
  assert.deepEqual(sent, ['rd1'], '再送は2回目を処理しない');
});

await t('processLineEvents: 返信失敗・例外時は予約を解いて再送で自己回復できる', async () => {
  let fail = true;
  const sent = [];
  const d = deps({ reply: async (token) => { if (fail) return false; sent.push(token); return true; } });
  const ev = { type: 'postback', replyToken: 'rf1', webhookEventId: 'W-retry-1', source: { type: 'user', userId: 'U1' }, postback: { data: 'stock:teatree20' } };
  await processLineEvents([ev], d);           // 返信失敗 → 予約解除
  fail = false;
  await processLineEvents([{ ...ev, replyToken: 'rf2' }], d);   // 再送は処理される
  assert.deepEqual(sent, ['rf2'], '失敗後の再送が通る');

  const d2 = deps({
    fetchStockLocations: async () => { throw new Error('down'); },
    reply: async (token) => { sent.push(token); return true; },
  });
  const ev2 = { type: 'postback', replyToken: 'rg1', webhookEventId: 'W-retry-2', source: { type: 'user', userId: 'U1' }, postback: { data: 'stock:teatree20' } };
  await processLineEvents([ev2], d2);         // 例外 → 予約解除
  await processLineEvents([{ ...ev2, replyToken: 'rg2' }], deps({ reply: async (token) => { sent.push(token); return true; } }));
  assert.ok(sent.includes('rg2'), '例外後の再送も通る');
});

await t('reserveEvent: 追い出し後の再予約を古いreleaseが消さない (トークン照合)', () => {
  const t0 = 9_000_000;
  const first = reserveEvent({ webhookEventId: 'W-evict' }, t0);
  assert.equal(first.duplicate, false);
  // DEDUP_MAX (5000) を超えるダミーIDで W-evict を追い出す
  for (let i = 0; i < 5001; i++) reserveEvent({ webhookEventId: `W-fill-${i}` }, t0 + 1);
  const second = reserveEvent({ webhookEventId: 'W-evict' }, t0 + 2);
  assert.equal(second.duplicate, false, '追い出し後は再予約できる');
  first.release();   // 古い予約のrelease
  const third = reserveEvent({ webhookEventId: 'W-evict' }, t0 + 3);
  assert.equal(third.duplicate, true, '新しい予約は古いreleaseで消えない');
});

await t('processLineEvents: 処理中の例外はイベント単位で握りつぶす', async () => {
  const sent = [];
  const d = deps({
    fetchStockSearch: async () => { throw new Error('boom'); },
    reply: async (token) => { sent.push(token); return true; },
  });
  await processLineEvents([
    { type: 'message', replyToken: 'rA', source: { type: 'user', userId: 'U1' }, message: { type: 'text', text: 'ティーツリー' } },
    { type: 'postback', replyToken: 'rB', source: { type: 'user', userId: 'U1' }, postback: { data: 'stock:teatree20' } },
  ], d);
  assert.deepEqual(sent, ['rB'], '1件目の例外が2件目を止めない');
});

console.log(`\ntest-line-search: ${passed} 件 pass`);
