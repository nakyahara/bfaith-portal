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
//        ssh -L 18080:localhost:8080 <VPS>  →  VPS_PROXY_URL=http://localhost:18080 YAHOO_TOKEN_MINT_SECRET=...
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

const BASE = 'https://circus.shopping.yahooapis.jp/ShoppingWebService/V1';
const SELLER = process.env.YAHOO_SELLER_ID;
if (!SELLER) { console.error('FATAL: YAHOO_SELLER_ID を指定してください'); process.exit(2); }

const args = process.argv.slice(2);
const argOf = f => { const i = args.indexOf(f); return i >= 0 ? args[i + 1] : null; };
const CONFIRM = process.env.CONTRACT_TEST_CONFIRM === 'yes';

// ── アクセストークン取得 ──
async function getToken() {
  if (process.env.YAHOO_ACCESS_TOKEN) return process.env.YAHOO_ACCESS_TOKEN;
  const proxy = process.env.VPS_PROXY_URL;
  const secret = process.env.YAHOO_TOKEN_MINT_SECRET;
  if (!proxy || !secret) {
    console.error('FATAL: YAHOO_ACCESS_TOKEN か (VPS_PROXY_URL + YAHOO_TOKEN_MINT_SECRET) を指定してください');
    process.exit(2);
  }
  // 平文HTTPで mint secret + 生トークンを流さない (Codexレビュー指摘)。localhost(SSHトンネル)は許可
  const isLoopback = /^https?:\/\/(localhost|127\.0\.0\.1)(:|\/|$)/.test(proxy);
  if (proxy.startsWith('http://') && !isLoopback && process.env.ALLOW_INSECURE_PROXY !== 'yes') {
    console.error('FATAL: 平文HTTPのプロキシ経由でトークン払い出しはできません。SSHトンネル (ssh -L 18080:localhost:8080 <VPS> → VPS_PROXY_URL=http://localhost:18080) を使うか、ALLOW_INSECURE_PROXY=yes を明示してください');
    process.exit(2);
  }
  const res = await fetch(`${proxy.replace(/\/$/, '')}/yahoo/access-token`, {
    method: 'POST', headers: { 'X-Proxy-Secret': secret },
  });
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
async function call(name, url, { method = 'GET', body = null, note = '' } = {}) {
  const wait = lastCall + 1100 - Date.now();
  if (wait > 0) await sleep(wait);
  lastCall = Date.now();
  const t0 = Date.now();
  const entry = { name, method, url, note, at: new Date().toISOString() };
  try {
    const res = await fetch(url, {
      method,
      headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
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
const topics = list1.body?.result?.topics || list1.body?.topics || [];
const total = list1.body?.result?.totalCount ?? list1.body?.totalCount;
results.push({ name: 'CHECK 一覧レスポンス構造', topicsFound: topics.length, totalCount: total ?? '(フィールド名要確認)' });
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
  const pick = e => JSON.stringify((e.body?.result?.messages || e.body?.messages || []).map(m => m.messageId));
  const a = pick(firstDetail.d), b = pick(again);
  results.push({ name: 'CHECK messageId 安定性', stable: a === b, first: a, again: b });
  console.log(`  CHECK messageId 安定性: ${a === b ? 'OK (同一)' : 'NG'}`);
}

// ── 4. エラー形 (--error-probes) ──
if (args.includes('--error-probes')) {
  await call('存在しないtopicId', `${BASE}/externalTalkDetail?sellerId=${encodeURIComponent(SELLER)}&topicId=bogus-topic-id-000`,
    { note: '404/400のレスポンス形' });
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
