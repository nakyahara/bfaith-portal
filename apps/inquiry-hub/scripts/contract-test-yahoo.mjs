// Step 0 契約テスト: Yahoo!ショッピング 問い合わせ管理API (externalTalk*)
// 設計書§3.3 の確認項目を実機で記録する。結果は contract-test-results/ にJSON保存 (gitignore済み・PII含むため共有時注意)
//
// 最重要目的のひとつ: **データセンターIPブロックの実測** (§3.3)
//   このスクリプトを (1) 自宅/会社PC (2) Render Web Shell の両方から実行し、statusを比較する。
//   Renderで403 → ローカルランナー or VPSプロキシ経由構成に確定。
//   ※ 注文APIは さくらVPS (vps-proxy) から稼働実績あり = システム系APIはDC-IP全滅ではない
//
// 認証トークンの取り方 (どちらか):
//   A) 既存VPSプロキシの /yahoo/access-token から払い出し (注文APIで運用中のYConnect+公開鍵認証を再利用)
//      ⚠️生トークンを扱うため平文HTTPでの取得は既定で拒否する。SSHトンネル経由を推奨:
//        ssh -L 18080:localhost:8080 <VPS>  →  VPS_PROXY_URL=http://localhost:18080 YAHOO_PROXY_SECRET=... YAHOO_TOKEN_MINT_SECRET=...
//      直接http://133.167.122.198:8080を使う場合は ALLOW_INSECURE_PROXY=yes が必要 (盗聴リスクを理解のうえで)
//   B) トークン直接指定: YAHOO_ACCESS_TOKEN=... YAHOO_SELLER_ID=... (VPS上でこのスクリプトを直接実行する場合など)
//
// 使い方 (read-only):
//   node apps/inquiry-hub/scripts/contract-test-yahoo.mjs
// オプション:
//   --error-probes                     不正topicId等のレスポンス形も記録
//   --mark-read <topicId>              ⚠️既読化 (状態変更。CONTRACT_TEST_CONFIRM=yes 必須)
//   ※ メッセージ投稿(返信)APIの契約テストは、テスト用質問を自作できた時点で追加する (雛形ではURL未確定のため未実装)
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const BASE = 'https://circus.shopping.yahooapis.jp/ShoppingWebService/V1';
const SELLER = process.env.YAHOO_SELLER_ID;
if (!SELLER) { console.error('FATAL: YAHOO_SELLER_ID を指定してください'); process.exit(2); }

const args = process.argv.slice(2);

// 公開鍵認証 (§3.1): 「sellerId:timestamp」をYahoo!発行の公開鍵でRSA暗号化して X-sws-signature に載せる。
// 実測 (2026-07-17): 署名なしBearerのみだと px-04102 'AccessToken has been expired. This API session is
// shorter than another API.' で401になる (注文APIと同じ扱い) ため、署名は実質必須。
// 署名なしの挙動をあえて記録したい場合のみ --no-signature を明示する。
const PUBKEY_PATH = process.env.YAHOO_PUBLIC_KEY_PATH || '';
const SIG_VERSION = process.env.YAHOO_SIGNATURE_VERSION || '4';
let PUBKEY = null;
if (!args.includes('--no-signature')) {
  if (!PUBKEY_PATH) {
    console.error('FATAL: YAHOO_PUBLIC_KEY_PATH を指定してください (署名なしは px-04102 で401になる実測あり)。署名なしの挙動確認は --no-signature を明示');
    process.exit(2);
  }
  try {
    PUBKEY = crypto.createPublicKey(fs.readFileSync(PUBKEY_PATH, 'utf-8'));
  } catch (e) {
    console.error(`FATAL: 公開鍵を読み込めません (${PUBKEY_PATH}): ${e.message}`);
    process.exit(2);
  }
}
function signatureHeaders() {
  if (!PUBKEY) return {};
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const encrypted = crypto.publicEncrypt(
    { key: PUBKEY, padding: crypto.constants.RSA_PKCS1_PADDING },
    Buffer.from(`${SELLER}:${timestamp}`, 'utf-8')
  );
  return { 'X-sws-signature': encrypted.toString('base64'), 'X-sws-signature-version': SIG_VERSION };
}

const argOf = f => { const i = args.indexOf(f); return i >= 0 ? args[i + 1] : null; };
const CONFIRM = process.env.CONTRACT_TEST_CONFIRM === 'yes';

// ── アクセストークン取得 ──
async function getToken() {
  if (process.env.YAHOO_ACCESS_TOKEN) return process.env.YAHOO_ACCESS_TOKEN;
  const proxy = process.env.VPS_PROXY_URL;
  const proxySecret = process.env.YAHOO_PROXY_SECRET;
  const mintSecret = process.env.YAHOO_TOKEN_MINT_SECRET;
  if (!proxy || !proxySecret || !mintSecret) {
    console.error('FATAL: YAHOO_ACCESS_TOKEN か (VPS_PROXY_URL + YAHOO_PROXY_SECRET + YAHOO_TOKEN_MINT_SECRET) を指定してください');
    console.error('  ※ /yahoo/access-token は X-Proxy-Secret (共通) と X-Token-Mint-Secret (専用) の両ヘッダが必要');
    process.exit(2);
  }
  // 平文HTTPで mint secret + 生トークンを流さない (Codexレビュー指摘)。localhost(SSHトンネル)は許可
  // URLパースで判定 (startsWithだと 'HTTP://' 等の表記揺れで迂回できる)
  let u;
  try { u = new URL(proxy); } catch { console.error(`FATAL: VPS_PROXY_URL が不正なURLです: ${proxy}`); process.exit(2); }
  const isLoopback = ['localhost', '127.0.0.1', '[::1]'].includes(u.hostname);
  if (u.protocol !== 'https:' && !isLoopback && process.env.ALLOW_INSECURE_PROXY !== 'yes') {
    console.error('FATAL: 平文HTTPのプロキシ経由でトークン払い出しはできません。SSHトンネル (ssh -L 18080:localhost:8080 <VPS> → VPS_PROXY_URL=http://localhost:18080) を使うか、ALLOW_INSECURE_PROXY=yes を明示してください');
    process.exit(2);
  }
  let res;
  try {
    res = await fetch(`${proxy.replace(/\/$/, '')}/yahoo/access-token`, {
      method: 'POST', headers: { 'X-Proxy-Secret': proxySecret, 'X-Token-Mint-Secret': mintSecret },
    });
  } catch (e) {
    console.error(`FATAL: プロキシに接続できません (${e.cause?.code || e.message})。SSHトンネルが起動しているか確認してください`);
    process.exit(2);
  }
  if (!res.ok) { console.error(`FATAL: トークン払い出し失敗 HTTP ${res.status} (本文はsecretを含みうるため表示しません)`); process.exit(2); }
  const j = await res.json();
  const token = j.access_token || j.accessToken || j.token;
  if (!token) { console.error(`FATAL: トークン払い出しレスポンスに access_token がありません (キー: ${Object.keys(j).join(',')})`); process.exit(2); }
  console.log('  (VPSプロキシからアクセストークン取得OK)');
  return token;
}
const TOKEN = await getToken();

const results = [];
const sleep = ms => new Promise(r => setTimeout(r, ms));
let lastCall = 0;

// 全呼び出しを1.1秒以上空ける (1クエリ/秒制限。§3.2)
async function call(name, url, { method = 'GET', body = null, note = '', expectError = false } = {}) {
  const wait = lastCall + 1100 - Date.now();
  if (wait > 0) await sleep(wait);
  lastCall = Date.now();
  const t0 = Date.now();
  const entry = { name, method, url, note, at: new Date().toISOString() };
  if (expectError) entry.expectError = true; // エラープローブ: 4xx/5xxが期待値なので終了判定から除外
  try {
    const res = await fetch(url, {
      method,
      headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json', ...signatureHeaders() },
      body: body ? JSON.stringify(body) : undefined,
    });
    entry.status = res.status;
    entry.ms = Date.now() - t0;
    const text = await res.text();
    try { entry.body = JSON.parse(text); } catch { entry.bodyText = text.slice(0, 2000); }
  } catch (e) {
    entry.error = String(e);
  }
  results.push(entry);
  console.log(`  [${entry.status ?? 'ERR'}] ${name} (${entry.ms ?? '-'}ms)${entry.error ? ' ' + entry.error : ''}`);
  return entry;
}

console.log(`Yahoo! 問い合わせ管理API 契約テスト (sellerId=${SELLER}, read-only${CONFIRM ? '+変更系許可' : ''})`);
console.log(`  実行元の判定用: このマシンから403が返る場合はDC-IPブロックの可能性`);

// ── 1. 質問一覧 (1ページ最大20件・受付日時降順。§3.1) ──
const list1 = await call('externalTalkList start=1', `${BASE}/externalTalkList?sellerId=${encodeURIComponent(SELLER)}&start=1&result=20`,
  { note: '総件数フィールド名/ソート順/isUnread/isCompleted の実形。403ならDC-IPブロック' });

// ── 2. ページング (2ページ目) ──
// 実測 (2026-07-17): 一覧は {summary: {filter, unansweredCount, topic: {start, end, count}}, headlines: [...]}
// headlines[] = {topicId, userIdx, isUnread, isUserUnRead, isNoAnswer, isCompleted, userPostTime, sellerPostTime,
//                qaType, isPrivate, category, title, body, messageCount, userMaskedId, itemCode, orderId, firstPoster, serviceType, memo}
// 形式ごとに「配列のパス」と「総件数のパス」を組で検証する (異なる形式の混在は不一致扱い)
const body1 = list1.body || {};
const LIST_FORMATS = [
  ['headlines', b => b.headlines, b => b.summary?.topic?.count],
  ['result.topics', b => b.result?.topics, b => b.result?.totalCount],
  ['topics', b => b.topics, b => b.totalCount],
];
let listFormat = null, topics = [], total = NaN;
for (const [fmtName, getArr, getCount] of LIST_FORMATS) {
  const arr = getArr(body1);
  if (Array.isArray(arr)) { listFormat = fmtName; topics = arr; total = getCount(body1); break; }
}
// 同期で使う主要フィールドの検証: 全行に topicId が無いと詳細/messageId検証が空振りして偽陽性になる
const topicIdOk = topics.every(t => t && typeof t.topicId === 'string' && t.topicId.length > 0);
const listOk = listFormat !== null && Number.isFinite(total) && topicIdOk && (total === 0 || topics.length > 0);
results.push({ name: 'CHECK 一覧レスポンス構造', ok: listOk, format: listFormat, topicsFound: topics.length,
  topicIdOk, totalCount: Number.isFinite(total) ? total : '(フィールド名要確認)' });
if (!listOk) console.error('  NG 一覧レスポンス構造が想定と不一致 (形式/件数/topicId のいずれか。契約変更の可能性。結果JSONを確認)');
if ((total ?? 0) > 20 || topics.length === 20) {
  await call('externalTalkList start=21', `${BASE}/externalTalkList?sellerId=${encodeURIComponent(SELLER)}&start=21&result=20`,
    { note: 'ページ境界の重複/欠落。降順ソート中の新着ズレは同期側のオーバーラップで吸収 (§8.1)' });
}

// ── 3. 質問詳細 (最大3件) + messageId 安定性 ──
const ids = topics.slice(0, 3).map(t => t.topicId).filter(Boolean);
let firstDetail = null;
for (const id of ids) {
  const d = await call(`externalTalkDetail ${String(id).slice(0, 12)}…`,
    `${BASE}/externalTalkDetail?sellerId=${encodeURIComponent(SELLER)}&topicId=${encodeURIComponent(id)}`,
    { note: 'messages[].messageId/postUserType/postdate(UNIX時間)/orderId/itemCode の実形' });
  if (!firstDetail) firstDetail = { id, d };
}
if (firstDetail) {
  const again = await call('externalTalkDetail (再取得)',
    `${BASE}/externalTalkDetail?sellerId=${encodeURIComponent(SELLER)}&topicId=${encodeURIComponent(firstDetail.id)}`,
    { note: 'messageId が再取得で同一か = 同期の外部ID安定性 (§3.3)' });
  // 実測 (2026-07-17): 詳細は {topic: {...}, messages: [{messageId(数値・トピック内連番), postUserType, postdate, body, fileList}]}
  // ID抽出できない場合 (構造不一致) を「安定」と誤認しないよう、抽出成功を先に検証する
  const extractIds = e => {
    const arr = e.body?.messages ?? e.body?.result?.messages ?? e.body?.posts ?? e.body?.details;
    if (!Array.isArray(arr) || arr.length === 0) return null;
    const ids = arr.map(m => m.messageId ?? m.postId ?? m.id);
    return ids.every(id => id !== null && id !== undefined) ? ids : null;
  };
  const a = extractIds(firstDetail.d), b = extractIds(again);
  const stable = a !== null && b !== null && JSON.stringify(a) === JSON.stringify(b);
  results.push({ name: 'CHECK messageId 安定性', stable, extracted: a !== null && b !== null,
    first: JSON.stringify(a), again: JSON.stringify(b) });
  console.log(`  CHECK messageId 安定性: ${stable ? 'OK (同一)' : (a === null || b === null ? 'NG (ID抽出不能=構造不一致)' : 'NG (再取得で変化)')}`);
}

// ── 4. エラー形 (--error-probes) ──
if (args.includes('--error-probes')) {
  await call('存在しないtopicId', `${BASE}/externalTalkDetail?sellerId=${encodeURIComponent(SELLER)}&topicId=bogus-topic-id-000`,
    { note: '404/400のレスポンス形。実測 (2026-07-17): 500 {error:{reason}}', expectError: true });
}

// ── 5. 変更系: 既読化 (CONTRACT_TEST_CONFIRM=yes + 明示指定のみ) ──
const readId = argOf('--mark-read');
if (readId) {
  if (!CONFIRM) {
    console.error('SKIP 既読化: 変更系は CONTRACT_TEST_CONFIRM=yes が必要です');
    results.push({ name: '既読化', skipped: 'CONTRACT_TEST_CONFIRM未設定' });
  } else {
    await call(`externalTalkRead ${readId}`, `${BASE}/externalTalkRead?topicId=${encodeURIComponent(readId)}`,
      { method: 'PUT', body: { sellerId: SELLER }, note: 'PUT+bodyにsellerId (公式仕様)。ステータスと反映を一覧再取得で確認' });
    await call('externalTalkList (既読化後)', `${BASE}/externalTalkList?sellerId=${encodeURIComponent(SELLER)}&start=1&result=20`,
      { note: 'isUnreadの反映確認' });
  }
}

// ── 保存 ──
const outDir = path.join(process.cwd(), 'contract-test-results');
fs.mkdirSync(outDir, { recursive: true });
const file = path.join(outDir, `yahoo-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
fs.writeFileSync(file, JSON.stringify({ ranAt: new Date().toISOString(), sellerId: SELLER, results }, null, 2));
console.log(`\n結果を保存: ${file}`);
console.log('※ 顧客情報を含みます。Gドライブ/リポジトリに置かないこと (契約テストノートには要点のみ転記)');
console.log('※ 同じスクリプトをRender Web Shellでも実行し、403の有無を比較すること (§3.3 DC-IP検証)');

// 「完走した=契約OK」の誤認防止: エラープローブ以外の全API呼び出しの失敗 + CHECK不合格を集計して exit 1
const callFailures = results.filter(r => !r.expectError && (r.error || (typeof r.status === 'number' && r.status >= 400)));
const checkFailures = results.filter(r => r.ok === false || r.stable === false);
if (callFailures.length || checkFailures.length) {
  console.error(`\nNG: 契約テストに失敗項目があります (API失敗 ${callFailures.length}件 / CHECK不合格 ${checkFailures.length}件): ` +
    [...callFailures, ...checkFailures].map(r => r.name).join(', '));
  process.exit(1);
}
