/**
 * Yahoo!問い合わせ対応漏れチェック のテスト。
 *   node apps/yahoo-inquiry-alert/tests/run-all.mjs
 * Yahoo API もプロキシも叩かない (fetchImpl を差し替えて検証する)。
 */
process.env.YAHOO_PROXY_URL = 'http://proxy.test';
process.env.YAHOO_PROXY_SECRET = 'test-secret';

const {
  QUERIES,
  MAX_SHOW,
  fmtJstFromUnixSec,
  validateListResponse,
  toItem,
  fetchInquiryStatus,
  buildMessage,
  proxyConfig,
} = await import('../service.js');

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
async function throws(fn, re, label) {
  try {
    await fn();
    fail++;
    console.error(`  ✗ ${label} (throw されなかった)`);
  } catch (e) {
    if (re.test(e.message)) { pass++; return; }
    fail++;
    console.error(`  ✗ ${label}\n      期待パターン: ${re}\n      実際: ${e.message}`);
  }
}
function section(name) { console.log(`\n── ${name}`); }

// テスト用の一覧レスポンスを組み立てる
function listJson({ filter, count, headlines }) {
  return {
    summary: { filter, unansweredCount: 0, topic: { start: 1, end: headlines.length, count } },
    headlines,
  };
}
function headline(over = {}) {
  return {
    topicId: over.topicId ?? 't-default',
    isUnread: false,
    isNoAnswer: false,
    isCompleted: false,
    title: 'テスト質問',
    userPostTime: 1754870400, // 2026-08-11 09:00 JST
    sellerPostTime: null,
    ...over,
  };
}

// fetchImpl スタブ: URLの requestFilter/serviceType (+start) を見て応答を返す。
// 'answered/shp@21' のように start つきキーが優先、無ければ 'answered/shp' に落ちる
function stubFetch(responses) {
  const calls = [];
  const impl = async (urlStr) => {
    const u = new URL(urlStr);
    const key = `${u.searchParams.get('requestFilter')}/${u.searchParams.get('serviceType')}`;
    calls.push({ key, params: Object.fromEntries(u.searchParams) });
    const r = responses[`${key}@${u.searchParams.get('start')}`] || responses[key];
    if (!r) throw new Error(`スタブ未定義: ${key}`);
    if (r.status && r.status !== 200) {
      return { status: r.status, text: async () => r.body ?? '<Error><Code>px-test</Code></Error>' };
    }
    return { status: 200, text: async () => JSON.stringify(r.json) };
  };
  impl.calls = calls;
  return impl;
}

const EMPTY = {
  'unanswered/shp': { json: listJson({ filter: 'unanswered', count: 0, headlines: [] }) },
  'unanswered/auc': { json: listJson({ filter: 'unanswered', count: 0, headlines: [] }) },
  'answered/shp': { json: listJson({ filter: 'answered', count: 0, headlines: [] }) },
  'answered/auc': { json: listJson({ filter: 'answered', count: 0, headlines: [] }) },
};

// ─── ユーティリティ ───
section('fmtJstFromUnixSec');
eq(fmtJstFromUnixSec(1754870400), '8/11 09:00', 'UNIX秒 → JST表記');
eq(fmtJstFromUnixSec(0), null, '0 は null');
eq(fmtJstFromUnixSec('abc'), null, '非数値は null');

section('proxyConfig');
{
  const c = proxyConfig({ YAHOO_PROXY_URL: 'http://x/', YAHOO_PROXY_SECRET: 's' });
  eq(c.base, 'http://x', '末尾スラッシュ除去');
  await throws(() => proxyConfig({}), /未設定/, 'secret 無しは throw');
}

// ─── レスポンス検証 (fail-closed) ───
section('validateListResponse');
{
  const q = QUERIES[0]; // unanswered/shp
  const good = listJson({ filter: 'unanswered', count: 1, headlines: [headline({ isNoAnswer: true })] });
  const r = validateListResponse(good, q);
  eq(r.count, 1, 'count を summary から読む');

  // 実測: Yahoo!は summary.filter を大文字で echo する ('UNANSWERED')
  const upper = listJson({ filter: 'UNANSWERED', count: 1, headlines: [headline({ isNoAnswer: true })] });
  eq(validateListResponse(upper, q).count, 1, '大文字の filter echo も受理する (実機実測)');

  await throws(() => validateListResponse(listJson({ filter: 'all', count: 0, headlines: [] }), q),
    /summary\.filter/, '旧プロキシ (filter=all) は contract error');
  await throws(() => validateListResponse({ summary: { filter: 'unanswered', topic: {} }, headlines: [] }, q),
    /count が読み取れません/, 'count 欠落は throw (0件と混同しない)');
  await throws(() => validateListResponse({ summary: { filter: 'unanswered', topic: { count: 1 } } }, q),
    /headlines/, 'headlines 欠落は throw');
  await throws(() => validateListResponse(
    listJson({ filter: 'unanswered', count: 1, headlines: [headline({ isNoAnswer: false })] }), q),
    /返信済み.*が混ざって/, 'unanswered に isNoAnswer=false が混ざったら throw');
  await throws(() => validateListResponse(
    listJson({ filter: 'unanswered', count: 1, headlines: [headline({ isNoAnswer: 'true' })] }), q),
    /真偽値/, 'isNoAnswer が文字列なら throw');

  // count と明細数の整合性 (Codexレビュー High: 矛盾レスポンスを 0件=無通知 にしない)
  await throws(() => validateListResponse(
    listJson({ filter: 'unanswered', count: 0, headlines: [headline({ isNoAnswer: true })] }), q),
    /明細が 1件/, 'count=0 なのに明細あり → throw');
  await throws(() => validateListResponse(
    listJson({ filter: 'unanswered', count: 5, headlines: [] }), q),
    /明細が 0件/, 'count>0 なのに明細なし → throw');
  await throws(() => validateListResponse(
    listJson({ filter: 'unanswered', count: 1, headlines: [headline({ topicId: 'x1', isNoAnswer: true }), headline({ topicId: 'x2', isNoAnswer: true })] }), q),
    /明細が 2件/, 'count より明細が多い → throw');
  {
    // count=30 (>20) のときは明細20件が期待値
    const h20 = Array.from({ length: 20 }, (_, i) => headline({ topicId: `p${i}`, isNoAnswer: true }));
    const r30 = validateListResponse(listJson({ filter: 'unanswered', count: 30, headlines: h20 }), q);
    eq(r30.count, 30, 'count>20 は明細20件で正常 (ページング)');
  }

  const qa = QUERIES[2]; // answered/shp → uncompleted
  await throws(() => validateListResponse(
    listJson({ filter: 'answered', count: 1, headlines: [headline({ isCompleted: true, sellerPostTime: 100 })] }), qa),
    /想定外の状態/, 'answered に完了済みが混ざったら throw (件数を信用できない)');
  await throws(() => validateListResponse(
    listJson({ filter: 'answered', count: 1, headlines: [headline({ serviceType: 'auc', sellerPostTime: 100 })] }), qa),
    /serviceType/, 'serviceType 不一致は throw');
  await throws(() => validateListResponse(
    listJson({ filter: 'answered', count: 1, headlines: [headline({})] }), qa),
    /sellerPostTime が不正/, 'answered の sellerPostTime null は throw (窓判定に使うため)');
}

// ─── toItem ───
section('toItem');
{
  const it = toItem(headline({ isNoAnswer: true, title: 'x', userPostTime: 100, sellerPostTime: '200' }), QUERIES[0]);
  eq(it.userPostTime, 100, 'userPostTime 数値化');
  eq(it.sellerPostTime, 200, 'sellerPostTime 文字列も数値化');
  eq(it.sortTime, 100, 'sort キー (unanswered は userPostTime)');
  const it2 = toItem(headline({ title: '', sellerPostTime: 300 }), QUERIES[2]);
  eq(it2.title, '(タイトルなし)', '空タイトルの表示');
  eq(it2.sortTime, 300, 'sort キー (answered は sellerPostTime)');
}

// ─── fetchInquiryStatus ───
// 基準時刻: 1754956800 = 2025-08-12 09:00 JST。30日窓の floor = 2025-07-13 09:00 JST
const NOW = new Date(1754956800 * 1000);

section('fetchInquiryStatus: 正常系');
{
  const impl = stubFetch({
    'unanswered/shp': { json: listJson({ filter: 'unanswered', count: 2, headlines: [
      headline({ topicId: 'u1', isNoAnswer: true, title: '在庫ありますか', userPostTime: 1754870400 }),
      headline({ topicId: 'u2', isNoAnswer: true, title: '色違いは', userPostTime: 1754784000 }),
    ] }) },
    'unanswered/auc': { json: listJson({ filter: 'unanswered', count: 1, headlines: [
      headline({ topicId: 'u3', isNoAnswer: true, title: '落札前質問', userPostTime: 1754956800 }),
    ] }) },
    'answered/shp': { json: listJson({ filter: 'answered', count: 1, headlines: [
      headline({ topicId: 'a1', title: '返信済みの件', userPostTime: 1754697600, sellerPostTime: 1754701200 }),
    ] }) },
    'answered/auc': { json: listJson({ filter: 'answered', count: 0, headlines: [] }) },
  });
  const r = await fetchInquiryStatus({ fetchImpl: impl, sleepMs: 0, now: NOW });
  eq(r.unanswered.count, 3, '未返信 count 合算 (shp2 + auc1)');
  eq(r.uncompleted.count, 1, '完了処理待ち count (窓内)');
  eq(r.uncompleted.olderCount, 0, '窓外なし');
  eq(r.windowDays, 30, '既定の窓は30日');
  eq(r.unanswered.items.map(i => i.topicId), ['u3', 'u1', 'u2'], '新しい順に混ぜてソート');
  eq(impl.calls.length, 4, '照会は4回 (shp/auc × unanswered/answered)');
  ok(impl.calls.every(c => c.params.sortOrder === 'desc'), '全照会 desc');
  ok(impl.calls.filter(c => c.key.startsWith('unanswered')).every(c => c.params.sort === 'userPostTime'),
    'unanswered は userPostTime ソート');
  ok(impl.calls.filter(c => c.key.startsWith('answered')).every(c => c.params.sort === 'sellerPostTime'),
    'answered は sellerPostTime ソート');
}

section('fetchInquiryStatus: 30日窓とページング');
{
  // 過去分バックログ: 窓内2件 + 窓外の古い返信。1ページ目の途中で floor に達したら打ち切る
  const oldSec = 1754956800 - 40 * 86400; // 40日前 = 窓外
  const impl = stubFetch({
    ...EMPTY,
    'answered/shp': { json: listJson({ filter: 'answered', count: 100, headlines: [
      headline({ topicId: 'w1', title: '窓内1', userPostTime: 1754697600, sellerPostTime: 1754701200 }),
      headline({ topicId: 'w2', title: '窓内2', userPostTime: 1754697600, sellerPostTime: 1754700000 }),
      ...Array.from({ length: 18 }, (_, i) => headline({ topicId: `old${i}`, title: '過去分', userPostTime: oldSec, sellerPostTime: oldSec - i })),
    ] }) },
  });
  const r = await fetchInquiryStatus({ fetchImpl: impl, sleepMs: 0, now: NOW });
  eq(r.uncompleted.count, 2, '窓内だけ要対応に数える');
  eq(r.uncompleted.olderCount, 98, '窓外は olderCount (100-2)');
  eq(r.uncompleted.truncated, false, '打ち切りなし');
  eq(impl.calls.filter(c => c.key === 'answered/shp').length, 1, 'floor到達でページングを止める');

  // 窓内が1ページを超える場合は次ページを読む (start=21)
  const inWin = i => headline({ topicId: `p${i}`, title: `窓内${i}`, userPostTime: 1754697600, sellerPostTime: 1754701200 - i });
  const impl2 = stubFetch({
    ...EMPTY,
    'answered/shp': { json: listJson({ filter: 'answered', count: 25, headlines: Array.from({ length: 20 }, (_, i) => inWin(i)) }) },
    'answered/shp@21': { json: {
      summary: { filter: 'answered', unansweredCount: 0, topic: { start: 21, end: 25, count: 25 } },
      headlines: Array.from({ length: 5 }, (_, i) => inWin(20 + i)),
    } },
  });
  const r2 = await fetchInquiryStatus({ fetchImpl: impl2, sleepMs: 0, now: NOW });
  eq(r2.uncompleted.count, 25, '窓内が20件超なら次ページも読む');
  eq(r2.uncompleted.olderCount, 0, '全件窓内なら olderCount=0');
  const starts = impl2.calls.filter(c => c.key === 'answered/shp').map(c => c.params.start);
  eq(starts, ['1', '21'], 'start=1→21 でページング');

  // ページ上限打ち切り: maxPages=1 で窓内がまだ続く → truncated
  const r3 = await fetchInquiryStatus({ fetchImpl: stubFetch({
    ...EMPTY,
    'answered/shp': { json: listJson({ filter: 'answered', count: 25, headlines: Array.from({ length: 20 }, (_, i) => inWin(i)) }) },
  }), sleepMs: 0, now: NOW, maxPages: 1 });
  eq(r3.uncompleted.truncated, true, '上限打ち切りは truncated=true (黙って切らない)');
  eq(r3.uncompleted.count, 20, '読めた分は数える');
  eq(r3.uncompleted.olderCount, 0, '打ち切り時は未読分を窓外 (olderCount) に混ぜない (過少表示防止)');

  // 走査中の総件数変動は fail-closed (どのページも信用できない)
  await throws(() => fetchInquiryStatus({ fetchImpl: stubFetch({
    ...EMPTY,
    'answered/shp': { json: listJson({ filter: 'answered', count: 25, headlines: Array.from({ length: 20 }, (_, i) => inWin(i)) }) },
    'answered/shp@21': { json: {
      summary: { filter: 'answered', unansweredCount: 0, topic: { start: 21, end: 24, count: 24 } },
      headlines: Array.from({ length: 4 }, (_, i) => inWin(20 + i)),
    } },
  }), sleepMs: 0, now: NOW }), /総件数が変動/, 'ページ間で count が動いたら throw');

  // 不正な opts は入口で止める (NaN が floorSec を汚染して全件窓内になる事故防止)
  await throws(() => fetchInquiryStatus({ fetchImpl: stubFetch(EMPTY), sleepMs: 0, now: NOW, windowDays: NaN }),
    /windowDays が不正/, 'windowDays=NaN は throw');
  await throws(() => fetchInquiryStatus({ fetchImpl: stubFetch(EMPTY), sleepMs: 0, now: NOW, maxPages: 0 }),
    /maxPages が不正/, 'maxPages=0 は throw');
  await throws(() => fetchInquiryStatus({ fetchImpl: stubFetch(EMPTY), sleepMs: 0, now: new Date('x') }),
    /now が不正/, '不正な Date は throw');

  // sellerPostTime 欠落は fail-closed (窓判定・打ち切り判定に使うため)
  await throws(() => fetchInquiryStatus({ fetchImpl: stubFetch({
    ...EMPTY,
    'answered/shp': { json: listJson({ filter: 'answered', count: 1, headlines: [
      headline({ topicId: 'x', title: 'x', userPostTime: 1754697600, sellerPostTime: null }),
    ] }) },
  }), sleepMs: 0, now: NOW }), /sellerPostTime が不正/, 'answered の sellerPostTime 欠落は throw');
}

section('fetchInquiryStatus: 異常系 (fail-closed)');
{
  await throws(
    () => fetchInquiryStatus({ fetchImpl: stubFetch({ ...EMPTY, 'unanswered/shp': { status: 403 } }), sleepMs: 0 }),
    /HTTP 403/, 'HTTP エラーは throw (0件にしない)');
  // 非JSONボディを返すケース
  const badJson = async () => ({ status: 200, text: async () => '<html>error</html>' });
  await throws(() => fetchInquiryStatus({ fetchImpl: badJson, sleepMs: 0 }), /JSONでありません/, 'HTMLレスポンスは throw');
  // 同じ topicId が shp と auc の両方に出る = serviceType が効いていない
  const dup = headline({ topicId: 'dup', isNoAnswer: true });
  await throws(
    () => fetchInquiryStatus({
      fetchImpl: stubFetch({
        ...EMPTY,
        'unanswered/shp': { json: listJson({ filter: 'unanswered', count: 1, headlines: [dup] }) },
        'unanswered/auc': { json: listJson({ filter: 'unanswered', count: 1, headlines: [dup] }) },
      }),
      sleepMs: 0,
    }),
    /同じ topicId/, 'shp/auc 重複は throw (serviceType 未転送の検出)');
  // fetch 自体の失敗
  const netErr = async () => { const e = new Error('ECONNREFUSED'); throw e; };
  await throws(() => fetchInquiryStatus({ fetchImpl: netErr, sleepMs: 0 }), /接続失敗/, 'ネットワーク失敗は throw');
}

// ─── buildMessage ───
section('buildMessage');
{
  eq(buildMessage({ unanswered: { count: 0, items: [] }, uncompleted: { count: 0, items: [] } }), null,
    '両方0件は null (通知しない)');

  const msg = buildMessage({
    unanswered: { count: 2, items: [
      { serviceType: 'shp', title: '在庫ありますか', userPostTime: 1754870400, sellerPostTime: null },
      { serviceType: 'auc', title: '落札前質問', userPostTime: 1754784000, sellerPostTime: null },
    ] },
    uncompleted: { count: 1, items: [
      { serviceType: 'shp', title: '返信済みの件', userPostTime: 1754697600, sellerPostTime: 1754701200 },
    ] },
  });
  ok(msg.includes('3件あります'), '合計件数');
  ok(msg.includes('未返信 (出店者回答待ち): *2件*'), '未返信の件数');
  ok(msg.includes('「完了する」が押されていない (直近30日): *1件*'), '完了処理待ちの件数 (窓表示つき)');
  ok(msg.includes('[通常] 在庫ありますか'), '通常タグ+タイトル');
  ok(msg.includes('[オークション] 落札前質問'), 'オークションタグ');
  ok(msg.includes('受信 8/9') && msg.includes('返信 8/9'), '完了処理待ちは受信+返信の両時刻');
  ok(!msg.includes('undefined') && !msg.includes('null'), 'undefined/null が漏れていない');

  // 上限超え: 明細は MAX_SHOW 件まで + 「他N件」
  const many = Array.from({ length: 25 }, (_, i) => (
    { serviceType: 'shp', title: `質問${i}`, userPostTime: 1754870400 - i, sellerPostTime: null }
  ));
  const msg2 = buildMessage({ unanswered: { count: 25, items: many }, uncompleted: { count: 0, items: [] } });
  eq((msg2.match(/^・/gm) || []).length, MAX_SHOW, `明細は ${MAX_SHOW} 件まで`);
  ok(msg2.includes(`他 ${25 - MAX_SHOW}件`), '超過分は件数で明示 (黙って切らない)');

  // 片方だけ0件ならそのセクションを出さない
  const msg3 = buildMessage({
    unanswered: { count: 0, items: [] },
    uncompleted: { count: 1, items: [{ serviceType: 'shp', title: 'x', userPostTime: 1754697600, sellerPostTime: 1754701200 }] },
  });
  ok(!msg3.includes('未返信'), '0件セクションは表示しない');
  ok(msg3.includes('1件あります'), '合計は完了処理待ちのみ');

  // 窓外の過去分は件数だけ添える / 過去分だけでは通知しない
  const msgOld = buildMessage({
    unanswered: { count: 1, items: [{ serviceType: 'shp', title: 'x', userPostTime: 1754870400, sellerPostTime: null }] },
    uncompleted: { count: 0, items: [], olderCount: 1036, truncated: false },
    windowDays: 30,
  });
  ok(msgOld.includes('30日より前の完了処理漏れが 1036件'), '窓外の過去分は件数で添える');
  ok(msgOld.includes('1件あります'), '合計には窓外を含めない');
  eq(buildMessage({
    unanswered: { count: 0, items: [] },
    uncompleted: { count: 0, items: [], olderCount: 1036, truncated: false },
    windowDays: 30,
  }), null, '窓外の過去分だけなら通知しない');
  const msgTrunc = buildMessage({
    unanswered: { count: 0, items: [] },
    uncompleted: { count: 1, items: [{ serviceType: 'shp', title: 'x', userPostTime: 1754697600, sellerPostTime: 1754701200 }], olderCount: 0, truncated: true },
    windowDays: 30,
  });
  ok(msgTrunc.includes('打ち切りました'), '上限打ち切りは通知文で明示');

  // 顧客入力タイトルの無害化 (Codexレビュー Medium)
  const evil = '偽装\nセクション\t<users/all> こんにちは';
  const msg5 = buildMessage({
    unanswered: { count: 1, items: [{ serviceType: 'shp', title: evil, userPostTime: 1754870400, sellerPostTime: null }] },
    uncompleted: { count: 0, items: [] },
  });
  ok(!/偽装\n/.test(msg5), '改行は除去される (セクション偽装防止)');
  ok(!msg5.includes('<users/all>'), 'メンション記法は分断される');
  ok(msg5.includes('偽装 セクション'), '本文自体は残る');

  // 長いタイトルの切り詰め
  const long = 'あ'.repeat(60);
  const msg4 = buildMessage({
    unanswered: { count: 1, items: [{ serviceType: 'shp', title: long, userPostTime: 1754870400, sellerPostTime: null }] },
    uncompleted: { count: 0, items: [] },
  });
  ok(msg4.includes('あ'.repeat(40) + '…'), 'タイトルは40文字で切り詰め');
}

// ─── notify-job (webhook ガード) ───
section('notify-job: webhook ガード');
{
  delete process.env.GCHAT_WEBHOOK_YAHOO_INQUIRY;
  const { runYahooInquiryAlert } = await import('../notify-job.js');
  await throws(() => runYahooInquiryAlert({}), /GCHAT_WEBHOOK_YAHOO_INQUIRY/,
    'webhook 未設定で --once は API を叩く前に throw');
  // dry-run は webhook 無しでも動く
  const r = await runYahooInquiryAlert({ dryRun: true, fetchImpl: stubFetch(EMPTY), sleepMs: 0 });
  eq(r.text, null, 'dry-run: 0件は text=null');
  ok(r.note.includes('dry-run'), 'dry-run 印字');
}

// ─── 通知送信 (fetch スタブで sendGChatMessage まで通す) ───
section('notify-job: 送信経路');
{
  process.env.GCHAT_WEBHOOK_YAHOO_INQUIRY = 'https://chat.googleapis.com/dummy';
  const { runYahooInquiryAlert } = await import('../notify-job.js');
  const sent = [];
  const origFetch = globalThis.fetch;
  globalThis.fetch = async (u, init) => {
    if (String(u).startsWith('https://chat.googleapis.com/')) {
      sent.push(JSON.parse(init.body));
      return { ok: true, status: 200, text: async () => '' };
    }
    return stubFetch({
      ...EMPTY,
      'unanswered/shp': { json: listJson({ filter: 'unanswered', count: 1, headlines: [
        headline({ topicId: 'n1', isNoAnswer: true, title: '通知テスト', userPostTime: 1754870400 }),
      ] }) },
    })(u);
  };
  try {
    const r = await runYahooInquiryAlert({ sleepMs: 0 });
    eq(sent.length, 1, '該当ありは webhook へ 1回 POST');
    ok(sent[0].text.includes('通知テスト'), '本文にタイトル');
    ok(r.ok, 'ok=true');

    sent.length = 0;
    globalThis.fetch = async (u) => {
      if (String(u).startsWith('https://chat.googleapis.com/')) throw new Error('通知先には行かないはず');
      return stubFetch(EMPTY)(u);
    };
    const r0 = await runYahooInquiryAlert({ sleepMs: 0 });
    eq(r0.text, null, '0件は通知なし (webhook POST されない)');
  } finally {
    globalThis.fetch = origFetch;
  }
}

console.log(`\n結果: pass=${pass} fail=${fail}`);
if (fail > 0) process.exit(1);
