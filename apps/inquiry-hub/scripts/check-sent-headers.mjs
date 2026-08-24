// 送信済みメールの実ヘッダ確認 (read-only)
//
// 目的 (2026-08-24 楽天マスクアドレス宛バウンスの原因確定):
//   楽天あんしんメルアド宛の返信が `552 5.2.0 <d.nakahara@b-faith.biz> sender rejected` で
//   バウンスした。Gmailのsend-asエイリアス (info@) は設定済みだったため、
//   「From自体が置き換わった」のか「Fromはinfo@だが封筒(Return-Path)/Senderにd.nakahara@が
//   出ている」のかを、実際に送られたメッセージのヘッダで確定させる。
//   ※メールディーラーは自前SMTP (封筒=info@) で同じ宛先に送れていた = RMS設定は封筒がinfo@なら足りる
//
// 使い方 (Render Web Shell。envはINQUIRY_GMAIL_*/PO_GMAIL_*のどちらでも):
//   DATA_DIR=/data node apps/inquiry-hub/scripts/check-sent-headers.mjs
//     → 楽天マスクアドレス宛の直近sentジョブ (最大3件) を自動で探して表示
//   node apps/inquiry-hub/scripts/check-sent-headers.mjs <GmailメッセージID...>
//     → 指定メッセージのみ (DATA_DIR不要)
import { resolveGmailTransportFromEnv, parseFromHeader } from '../sync/adapters/gmail.js';

const t = resolveGmailTransportFromEnv();
if (!t) { console.error('FATAL: INQUIRY_GMAIL_* / PO_GMAIL_* が未設定です'); process.exit(2); }

async function accessToken() {
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ client_id: t.clientId, client_secret: t.clientSecret, refresh_token: t.refreshToken, grant_type: 'refresh_token' }).toString(),
  });
  const j = await res.json();
  if (!res.ok || !j.access_token) throw new Error(`トークン取得失敗 HTTP ${res.status}: ${j.error_description || j.error || '不明'}`);
  return j.access_token;
}
const TOKEN = await accessToken();
const get = async (path) => {
  const res = await fetch(`https://gmail.googleapis.com/gmail/v1/users/me/${path}`, { headers: { Authorization: `Bearer ${TOKEN}` } });
  const j = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`HTTP ${res.status} (${path.slice(0, 60)}): ${j?.error?.message || '不明'}`);
  return j;
};

const profile = await get('profile');
console.log(`認証アカウント: ${profile.emailAddress}`);

// 対象メッセージIDの決定: 引数優先、無ければDBから楽天マスクアドレス宛のsentジョブを探す
let targets = process.argv.slice(2).map(id => ({ id, note: '(引数指定)' }));
if (!targets.length) {
  const { getDB } = await import('../db.js');
  const rows = getDB().prepare(`SELECT o.id AS job_id, o.external_reply_id, o.sent_at, o.error_message,
      i.customer_identifier, i.subject
    FROM outbox_replies o JOIN inquiries i ON i.id = o.inquiry_id
    WHERE o.status = 'sent' AND i.channel_type = 'email'
      AND i.customer_identifier LIKE '%fw.rakuten.ne.jp%'
    ORDER BY o.id DESC LIMIT 3`).all();
  if (!rows.length) {
    console.log('楽天マスクアドレス宛のsentジョブが見つかりません (引数でGmailメッセージIDを指定してください)');
    process.exit(1);
  }
  targets = rows.map(r => ({
    id: r.external_reply_id,
    note: `(ジョブ#${r.job_id} ${r.sent_at} 宛先=${r.customer_identifier} 件名=${String(r.subject || '').slice(0, 30)})`,
  }));
}

const HEADERS = ['From', 'Sender', 'To', 'Reply-To', 'Return-Path', 'Message-ID', 'Date', 'Subject'];
let verdictFrom = null;
for (const tg of targets) {
  console.log(`\n── メッセージ ${tg.id} ${tg.note}`);
  let m;
  try {
    m = await get(`messages/${encodeURIComponent(tg.id)}?format=metadata` + HEADERS.map(h => `&metadataHeaders=${h}`).join(''));
  } catch (e) {
    console.log(`  取得失敗: ${String(e?.message || e)}`);
    continue;
  }
  const h = name => (m.payload?.headers || []).find(x => x.name.toLowerCase() === name.toLowerCase())?.value || '(なし)';
  for (const name of HEADERS) console.log(`  ${name}: ${h(name).slice(0, 120)}`);
  const from = parseFromHeader(h('From'));
  if (from.mailbox) verdictFrom = from.mailbox;
}

// 判定の目安を出す (最後に取得できたメッセージのFromで判断)
if (verdictFrom) {
  console.log('\n── 判定の目安');
  if (verdictFrom === 'info@b-faith.biz') {
    console.log('  Fromヘッダは info@b-faith.biz で正しい。');
    console.log('  → 楽天が拒否した d.nakahara@ は SMTP封筒 (Return-Path) か Sender ヘッダ由来。');
    console.log('    Gmail API経由の送信では封筒が認証アカウントになるため、コード・Gmail設定では変えられない。');
    console.log('    対策: ①RMS店舗連絡先に d.nakahara@b-faith.biz を追加 (最速)');
    console.log('          ②info@ を実メールボックス化して INQUIRY_GMAIL_* 専用トークンに切替 (根本・封筒=info@になる)');
  } else {
    console.log(`  ⚠️ Fromヘッダが ${verdictFrom} になっている (エイリアスがAPI送信で効いていない)。`);
    console.log('  → Gmail設定「他のメールアドレスからメールを送信」の info@ の状態 (確認済みか・エイリアス扱いか) を確認。');
  }
}
