// 楽天受信同期アダプターのスモーク: HTTPをモックして契約 (URL組立/ページング/マッピング/失敗時全体throw) を検証
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-sync-rakuten.mjs
// DATA_DIR 直下は使わず毎回ユニークな一時サブディレクトリを作る (既存ファイルは一切削除しない。
// 本番 DATA_DIR を誤指定しても実DBに触れない。Codex R1 high)
import fs from 'fs';
import path from 'path';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です。作業ディレクトリを指定してください (例: DATA_DIR=c:/tmp/ih-rakuten-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-rakuten-'));
process.env.DATA_DIR = workDir;

// ⚠️ db.js は import 時点で DATA_DIR を定数化するため、env を一時ディレクトリへ差し替えた後に
// 動的 import する (静的 import だと本番 DATA_DIR 誤指定時に実DBへ書いてしまう。Codex R2 high)
const { initInquiryHubDB, getDB } = await import('./db.js');
const { runSync } = await import('./sync/engine.js');
const { createRakutenAdapter, mapInquiry, toJstNaive, DEFAULT_LOOKBACK_DAYS } = await import('./sync/adapters/rakuten.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

// ─── フィクスチャ ───
const CRED = { serviceSecret: 'sec', licenseKey: 'key', sleepMs: 0 };
const row = (over = {}) => ({
  inquiryNumber: '373343-20260617-00000001o',
  shopId: 373343,
  userName: '楽天太郎',
  userMaskEmail: 'ma***@example.com',
  message: '在庫はありますか',
  regDate: '2026-07-15T10:00:00+09:00',
  lastUpdateDate: '2026-07-15T10:00:00+09:00',
  itemUrl: null, itemName: 'テスト商品', itemNumber: 'test-sku-1',
  isCompleted: false, completedDate: null,
  category: '再入荷・在庫', type: '商品問い合わせ',
  orderNumber: 'ORD-001', readByMerchant: false,
  attachments: [],
  replies: [],
  isMessageDeleted: false, socialGiftUserType: null,
  ...over,
});
const pageBody = (list, totalPageCount = 1, page = 1) =>
  ({ totalCount: list.length, totalPageCount, page, list });

/** URL→レスポンスのモックfetch。呼び出しURLを記録する */
function mockFetch(handler) {
  const calls = [];
  const fn = async (url) => {
    calls.push(url);
    const r = handler(url, calls.length);
    if (r instanceof Error) throw r;
    return {
      status: r.status ?? 200,
      json: async () => r.body,
      text: async () => JSON.stringify(r.body ?? ''),
    };
  };
  fn.calls = calls;
  return fn;
}
const paramOf = (url, key) => new URL(url).searchParams.get(key);

// ─── 1. toJstNaive ───
console.log('1. JST変換');
check('UTC→JST naive (+9h)', toJstNaive('2026-07-17T00:30:00Z') === '2026-07-17T09:30:00');
check('日跨ぎ', toJstNaive('2026-07-16T16:00:00Z') === '2026-07-17T01:00:00');
check('1桁月日のゼロ埋め', toJstNaive('2026-01-02T03:04:05Z') === '2026-01-02T12:04:05');

// ─── 2. fetchNew の窓組立 (lookback 拡張) ───
console.log('2. 窓組立');
{
  const f = mockFetch(() => ({ body: pageBody([]) }));
  const ad = createRakutenAdapter({ ...CRED, fetchImpl: f });
  const until = '2026-07-17T03:00:00Z';
  const since = '2026-07-17T02:00:00Z'; // committed_until−60分 相当 (lookbackより十分新しい)
  await ad.fetchNew({ sinceIso: since, untilIso: until });
  check('lookback 60日は30日スライス2本に分割', f.calls.length === 2, `calls=${f.calls.length}`);
  const from = paramOf(f.calls[0], 'fromDate');
  check('スライス1 fromDate = until−60日',
    from === toJstNaive(Date.parse(until) - DEFAULT_LOOKBACK_DAYS * 86400000), `from=${from}`);
  check('スライス2 fromDate = until−30日',
    paramOf(f.calls[1], 'fromDate') === toJstNaive(Date.parse(until) - 30 * 86400000));
  check('スライス2 toDate = untilIso のJST表記', paramOf(f.calls[1], 'toDate') === '2026-07-17T12:00:00');
  check('スライス連続 (1本目のto = 2本目のfrom)', paramOf(f.calls[0], 'toDate') === paramOf(f.calls[1], 'fromDate'));
  // API上限 (31日以内) をどのスライスも超えない
  const jstMs = s => Date.parse(s + '+09:00');
  check('全スライスが31日以内', f.calls.every(u =>
    jstMs(paramOf(u, 'toDate')) - jstMs(paramOf(u, 'fromDate')) <= 31 * 86400000));
  check('limit=50 固定', paramOf(f.calls[0], 'limit') === '50');

  // since が lookback より古い (初回バックフィル等) 場合はそのまま使う + 端数スライス
  const f2 = mockFetch(() => ({ body: pageBody([]) }));
  const ad2 = createRakutenAdapter({ ...CRED, fetchImpl: f2 });
  const oldSince = '2026-05-03T03:00:00Z'; // 75日前 → 30+30+15日の3スライス
  await ad2.fetchNew({ sinceIso: oldSince, untilIso: until });
  check('75日窓は3スライス', f2.calls.length === 3, `calls=${f2.calls.length}`);
  check('lookbackより古いsinceは維持', paramOf(f2.calls[0], 'fromDate') === toJstNaive(oldSince));
  check('端数スライスの終端 = untilIso', paramOf(f2.calls[2], 'toDate') === '2026-07-17T12:00:00');
}

// ─── 3. マッピング ───
console.log('3. マッピング');
{
  const r = mapInquiry(row({
    isCompleted: true, readByMerchant: true,
    attachments: [{ label: 'photo.jpg', path: 'att/aaa.jpg' }],
    replies: [
      { id: 0, message: 'ございます', regDate: '2026-07-15T11:00:00+09:00', replyFrom: 'merchant', isRead: true, attachments: [], isMessageDeleted: false },
      { id: 1, message: 'では注文します', regDate: '2026-07-15T12:00:00+09:00', replyFrom: 'user', isRead: false, attachments: [{ label: 'order.png', path: 'att/bbb.png' }], isMessageDeleted: false },
    ],
  }));
  check('externalInquiryId=inquiryNumber', r.externalInquiryId === '373343-20260617-00000001o');
  check('subject=カテゴリ（種別）', r.subject === '再入荷・在庫（商品問い合わせ）');
  check('externalStatus completed', r.externalStatus === 'completed');
  check('externalIsRead true', r.externalIsRead === true);
  check('注文/商品の紐付け', r.orderNumber === 'ORD-001' && r.productCode === 'test-sku-1' && r.productName === 'テスト商品');
  check('customerIdentifier=userMaskEmail', r.customerIdentifier === 'ma***@example.com');
  check('メッセージ3件 (質問+返信2)', r.messages.length === 3);
  const [q, m0, m1] = r.messages;
  check('質問: id=q / customer / incoming', q.externalMessageId === 'q' && q.senderType === 'customer' && q.isIncoming === 1);
  check('返信0: id=r:0 / merchant→shop / outgoing', m0.externalMessageId === 'r:0' && m0.senderType === 'shop' && m0.isIncoming === 0);
  check('返信1: id=r:1 / user→customer / incoming', m1.externalMessageId === 'r:1' && m1.senderType === 'customer' && m1.isIncoming === 1);
  check('添付: path→externalAttachmentId / label→fileName',
    q.attachments[0].externalAttachmentId === 'att/aaa.jpg' && q.attachments[0].fileName === 'photo.jpg'
    && m1.attachments[0].externalAttachmentId === 'att/bbb.png');
  check('externalStatus open (未完了)', mapInquiry(row()).externalStatus === 'open');
  check('reply.id 欠落は undefined (エンジンsynthetic委譲)',
    mapInquiry(row({ replies: [{ message: 'x', regDate: '2026-07-15T11:00:00+09:00', replyFrom: 'merchant' }] })).messages[1].externalMessageId === undefined);
  let threw = false;
  try { mapInquiry({}); } catch (e) { threw = e.errorType === 'contract_violation'; }
  check('inquiryNumber欠落は contract_violation', threw);
  check('expectedShopId一致はOK', mapInquiry(row(), '373343').externalInquiryId !== undefined);
  check('expectedShopId一致 (数値/文字列差異を吸収)', mapInquiry(row(), 373343).externalInquiryId !== undefined);
  let shopMismatch = false;
  try { mapInquiry(row(), '999999'); } catch (e) { shopMismatch = e.errorType === 'contract_violation'; }
  check('shopId不一致は contract_violation (他店舗混入防止)', shopMismatch);
  let badFrom = false;
  try { mapInquiry(row({ replies: [{ id: 0, message: 'x', regDate: '2026-07-15T11:00:00+09:00', replyFrom: 'admin' }] })); }
  catch (e) { badFrom = e.errorType === 'contract_violation'; }
  check('未知のreplyFromは contract_violation (誤未読化防止)', badFrom);
}

// ─── 4. ページング ───
console.log('4. ページング');
{
  const rows = [row(), row({ inquiryNumber: 'n-2' }), row({ inquiryNumber: 'n-3' })];
  const f = mockFetch((url) => {
    const page = Number(paramOf(url, 'page'));
    if (page === 1) return { body: pageBody([rows[0], rows[1]], 2, 1) };
    return { body: pageBody([rows[1], rows[2]], 2, 2) }; // ページ跨ぎ重複 (並びズレ模倣)
  });
  const ad = createRakutenAdapter({ ...CRED, fetchImpl: f });
  const r = await ad.fetchNew({ sinceIso: '2026-07-17T02:00:00Z', untilIso: '2026-07-17T03:00:00Z' });
  check('全スライス×全ページ取得 (2スライス×2ページ=4リクエスト)', f.calls.length === 4, `calls=${f.calls.length}`);
  check('重複排除して3件', r.inquiries.length === 3);
}

// ─── 5. 失敗は全体 throw (部分成功を返さない) ───
console.log('5. 失敗系');
{
  const f = mockFetch((url) => {
    const page = Number(paramOf(url, 'page'));
    if (page === 1) return { body: pageBody([row()], 3, 1) };
    return { status: 500, body: { error: 'boom' } };
  });
  const ad = createRakutenAdapter({ ...CRED, fetchImpl: f });
  let threw = null;
  try { await ad.fetchNew({ sinceIso: '2026-07-17T02:00:00Z', untilIso: '2026-07-17T03:00:00Z' }); }
  catch (e) { threw = e; }
  check('2ページ目失敗で全体throw', threw !== null && threw.errorType === 'fetch_failed');

  const f401 = mockFetch(() => ({ status: 401, body: { error: 'auth' } }));
  let e401 = null;
  try { await createRakutenAdapter({ ...CRED, fetchImpl: f401 }).fetchNew({ sinceIso: '2026-07-17T02:00:00Z', untilIso: '2026-07-17T03:00:00Z' }); }
  catch (e) { e401 = e; }
  check('401 は errorType=auth (licenseKey期限切れ検知)', e401?.errorType === 'auth');

  const f429 = mockFetch(() => ({ status: 429, body: {} }));
  let e429 = null;
  try { await createRakutenAdapter({ ...CRED, fetchImpl: f429 }).fetchNew({ sinceIso: '2026-07-17T02:00:00Z', untilIso: '2026-07-17T03:00:00Z' }); }
  catch (e) { e429 = e; }
  check('429 は errorType=rate_limited', e429?.errorType === 'rate_limited');

  const fBad = mockFetch(() => ({ body: { unexpected: true } }));
  let eBad = null;
  try { await createRakutenAdapter({ ...CRED, fetchImpl: fBad }).fetchNew({ sinceIso: '2026-07-17T02:00:00Z', untilIso: '2026-07-17T03:00:00Z' }); }
  catch (e) { eBad = e; }
  check('契約違反レスポンスは contract_violation', eBad?.errorType === 'contract_violation');

  const fMany = mockFetch(() => ({ body: pageBody([row()], 999, 1) }));
  let eMany = null;
  try { await createRakutenAdapter({ ...CRED, fetchImpl: fMany, maxPages: 3 }).fetchNew({ sinceIso: '2026-07-17T02:00:00Z', untilIso: '2026-07-17T03:00:00Z' }); }
  catch (e) { eMany = e; }
  check('maxPages超過は window_too_large', eMany?.errorType === 'window_too_large');

  let eCred = null;
  try { createRakutenAdapter({ sleepMs: 0 }); } catch (e) { eCred = e; }
  check('認証情報なしは生成時に throw', eCred !== null);

  // warehouse transport: URL/ヘッダの組立 (Renderに楽天キーを置かない設計原則)
  const fWh = (() => {
    const calls = [];
    const fn = async (url, opts) => {
      calls.push({ url, headers: opts.headers });
      return { status: 200, json: async () => pageBody([]), text: async () => '' };
    };
    fn.calls = calls;
    return fn;
  })();
  const adWh = createRakutenAdapter({
    transport: 'warehouse', warehouseUrl: 'https://wh.example.com/', serviceToken: 'tok',
    cfClientId: 'cfid', cfClientSecret: 'cfsec', sleepMs: 0, fetchImpl: fWh,
  });
  await adWh.fetchNew({ sinceIso: '2026-07-17T02:00:00Z', untilIso: '2026-07-17T03:00:00Z' });
  check('warehouse: passthrough URLを叩く (末尾スラッシュ吸収)',
    fWh.calls[0].url.startsWith('https://wh.example.com/service-api/rakuten-rms/inquiries?fromDate='), fWh.calls[0].url);
  check('warehouse: CF Access + Bearerヘッダ',
    fWh.calls[0].headers['CF-Access-Client-Id'] === 'cfid'
    && fWh.calls[0].headers['CF-Access-Client-Secret'] === 'cfsec'
    && fWh.calls[0].headers.Authorization === 'Bearer tok');
  check('warehouse: 楽天キーなしで生成できる', true);
  let eWhCfg = null;
  try { createRakutenAdapter({ transport: 'warehouse', warehouseUrl: 'x', sleepMs: 0 }); } catch (e) { eWhCfg = e; }
  check('warehouse: 設定不足は生成時に throw', eWhCfg !== null);
  let eBadTransport = null;
  try { createRakutenAdapter({ transport: 'nope', sleepMs: 0 }); } catch (e) { eBadTransport = e; }
  check('未知transportは生成時に throw', eBadTransport !== null);

  // タイムアウト (AbortSignal.timeout 発火) は fetch_failed に分類される
  const timeoutErr = new Error('The operation was aborted due to timeout');
  timeoutErr.name = 'TimeoutError';
  const fTimeout = mockFetch(() => timeoutErr);
  let eTimeout = null;
  try { await createRakutenAdapter({ ...CRED, fetchImpl: fTimeout }).fetchNew({ sinceIso: '2026-07-17T02:00:00Z', untilIso: '2026-07-17T03:00:00Z' }); }
  catch (e) { eTimeout = e; }
  check('タイムアウトは fetch_failed + メッセージ明示', eTimeout?.errorType === 'fetch_failed' && /タイムアウト/.test(eTimeout?.message || ''));
}

// ─── 6. エンジン結合 (runSync 経由・冪等性・追い返信) ───
console.log('6. エンジン結合');
{
  const shopId = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('rakuten','楽天店','373343')").run().lastInsertRowid;
  const T0 = Date.parse('2026-07-17T03:00:00Z');
  let dataset = [row()];
  const f = mockFetch(() => ({ body: pageBody(dataset) }));
  const ad = createRakutenAdapter({ ...CRED, fetchImpl: f });

  const r1 = await runSync(shopId, ad, { now: T0 });
  check('初回同期: 1問い合わせ+1メッセージ', r1.ok && r1.stats.newInquiries === 1 && r1.stats.newMessages === 1, JSON.stringify(r1));

  const r2 = await runSync(shopId, ad, { now: T0 + 15 * 60000 });
  check('再同期は冪等 (新規0)', r2.ok && r2.stats.newInquiries === 0 && r2.stats.newMessages === 0, JSON.stringify(r2));

  // 店舗返信→顧客の追い返信が同じ一覧行の replies[] に増える (regDate窓では古い行のまま)
  dataset = [row({
    replies: [
      { id: 0, message: 'ございます', regDate: '2026-07-17T12:30:00+09:00', replyFrom: 'merchant' },
      { id: 1, message: '追加質問です', regDate: '2026-07-17T13:00:00+09:00', replyFrom: 'user' },
    ],
  })];
  const r3 = await runSync(shopId, ad, { now: T0 + 30 * 60000 });
  check('追い返信2件を取り込み (lookback再照合)', r3.ok && r3.stats.newInquiries === 0 && r3.stats.newMessages === 2, JSON.stringify(r3));

  const inq = db.prepare('SELECT * FROM inquiries WHERE shop_id = ?').get(shopId);
  check('is_unread=1 (顧客新着)', inq.is_unread === 1);
  check('conversation_rev=3', inq.conversation_rev === 3);
  const msgs = db.prepare('SELECT external_message_id, sender_type, is_incoming FROM inquiry_messages WHERE inquiry_id = ? ORDER BY id').all(inq.id);
  check('メッセージID = q / r:0 / r:1', msgs.map(m => m.external_message_id).join(',') === 'q,r:0,r:1');
  check('sent_at がUTC正準形式', /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(
    db.prepare('SELECT sent_at FROM inquiry_messages WHERE inquiry_id = ? AND external_message_id = ?').get(inq.id, 'q').sent_at));
  check('JST→UTC変換が正しい (10:00+09:00 → 01:00Z)',
    db.prepare('SELECT sent_at FROM inquiry_messages WHERE inquiry_id = ? AND external_message_id = ?').get(inq.id, 'q').sent_at === '2026-07-15T01:00:00Z');
}

// ガードの自己検証: DBが一時サブディレクトリ「のみ」に作られていること (ベース直下に漏れたら即FAIL)
check('DBは一時サブディレクトリのみに作成 (ベース直下に漏れない)',
  fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && !fs.existsSync(path.join(baseDir, 'inquiry-hub.db')));

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true }); // 自分が作った一時サブディレクトリのみ削除
process.exitCode = failed ? 1 : 0;
