// Step 3 (メールチャネル) 契約テスト: Gmail API 疎通 + info@b-faith.biz の可視性実証 (read-only)
//
// 目的: OAuthトークンは d.nakahara@b-faith.biz だが、info@ 宛メールが転送で同じ受信箱に
//       入っている運用のため、「info@ 宛のメールがこのトークンから見えること」を実測する。
//
// 使い方 (Render Web Shell。env は INQUIRY_GMAIL_* / PO_GMAIL_* のどちらでも):
//   node apps/inquiry-hub/scripts/contract-test-gmail.mjs
// 出力は件数とヘッダ要約のみ (本文は取得しない)
import { resolveGmailTransportFromEnv } from '../sync/adapters/gmail.js';

const t = resolveGmailTransportFromEnv();
if (!t) { console.error('FATAL: INQUIRY_GMAIL_* / PO_GMAIL_* が未設定です'); process.exit(2); }

async function accessToken() {
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ client_id: t.clientId, client_secret: t.clientSecret, refresh_token: t.refreshToken, grant_type: 'refresh_token' }).toString(),
  });
  const j = await res.json();
  if (!res.ok || !j.access_token) throw new Error(`トークン取得失敗 HTTP ${res.status}: ${j.error_description || j.error || '不明'}`);
  console.log(`トークンOK (scope: ${j.scope || '(不明)'})`);
  return j.access_token;
}
const TOKEN = await accessToken();
const get = async (path) => {
  const res = await fetch(`https://gmail.googleapis.com/gmail/v1/users/me/${path}`, { headers: { Authorization: `Bearer ${TOKEN}` } });
  const j = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`HTTP ${res.status} (${path.slice(0, 40)}): ${j?.error?.message || '不明'}`);
  return j;
};

// 1. プロフィール (トークンのメールボックス確認)
const profile = await get('profile');
console.log(`\n1. メールボックス: ${profile.emailAddress} (総メッセージ ${profile.messagesTotal}件)`);

// 2. 直近7日の受信量
const q = s => `messages?q=${encodeURIComponent(s)}&maxResults=1`;
const recent = await get(q('-in:chats newer_than:7d'));
console.log(`2. 直近7日のメール: 推定 ${recent.resultSizeEstimate}件`);

// 3. info@ 宛の可視性 (最重要)
const infoTo = await get(q('to:info@b-faith.biz newer_than:7d'));
const infoDeliver = await get(q('deliveredto:info@b-faith.biz newer_than:7d'));
console.log(`3. info@b-faith.biz 宛 (直近7日): to:検索 ${infoTo.resultSizeEstimate}件 / deliveredto:検索 ${infoDeliver.resultSizeEstimate}件`);
if ((infoTo.resultSizeEstimate || 0) === 0 && (infoDeliver.resultSizeEstimate || 0) === 0) {
  console.log('   ⚠️ info@宛メールが見つかりません。転送設定を確認してください (このままだとinfo@宛の問い合わせを取り込めない)');
} else {
  console.log('   ✅ info@宛メールがこのトークンから見えています');
}

// 4. 最新のinfo@宛メールのヘッダ要約 (本文は取得しない)
const sample = await get(`messages?q=${encodeURIComponent('to:info@b-faith.biz newer_than:7d')}&maxResults=1`);
if (sample.messages?.length) {
  const m = await get(`messages/${sample.messages[0].id}?format=metadata&metadataHeaders=From&metadataHeaders=To&metadataHeaders=Subject&metadataHeaders=Delivered-To`);
  const h = name => (m.payload?.headers || []).find(x => x.name.toLowerCase() === name.toLowerCase())?.value || '(なし)';
  console.log(`4. 最新のinfo@宛メール: From=${h('From').slice(0, 60)} / To=${h('To').slice(0, 60)} / 件名=${h('Subject').slice(0, 40)}`);
  console.log(`   threadId=${m.threadId} internalDate=${new Date(Number(m.internalDate)).toISOString()}`);
}

// 5. after: クエリの動作 (同期ウィンドウの前提)
const dayAgoSec = Math.floor(Date.now() / 1000) - 86400;
const afterQ = await get(q(`-in:chats after:${dayAgoSec}`));
console.log(`5. after:<epoch秒> クエリ: 直近24h 推定 ${afterQ.resultSizeEstimate}件 (同期ウィンドウ方式OK)`);

console.log('\n完了 (read-only。何も変更していません)');
