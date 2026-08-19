// Step 0 契約テスト: 楽天 問い合わせ管理API (inquirymng-api / R-Messe)
// 設計書§2.4 の確認項目を実機で記録する。結果は contract-test-results/ にJSON保存 (gitignore済み・PII含むため共有時注意)
//
// 前提: RMSで WEB API利用申請 + 問い合わせ管理APIの利用機能追加 + R-Messe利用申込が完了していること
//
// 使い方 (read-only。何も変更しない):
//   RAKUTEN_SERVICE_SECRET=... RAKUTEN_LICENSE_KEY=... node apps/inquiry-hub/scripts/contract-test-rakuten.mjs
// オプション:
//   --days 30                     取得対象期間 (既定30日)
//   --error-probes                認証エラー/存在しない問い合わせ番号のレスポンス形も記録
//   --send-reply <番号> --message "テキスト"   ⚠️実返信を送る (自作のテスト問い合わせ限定!)
//   --mark-read <番号> / --complete <番号> / --incomplete <番号>   ⚠️状態を変更する
//   ※ 変更系は環境変数 CONTRACT_TEST_CONFIRM=yes が無いと実行されない
import fs from 'fs';
import path from 'path';

const BASE = 'https://api.rms.rakuten.co.jp/es/1.0/inquirymng-api';
const SECRET = process.env.RAKUTEN_SERVICE_SECRET;
const KEY = process.env.RAKUTEN_LICENSE_KEY;
if (!SECRET || !KEY) {
  console.error('FATAL: RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY を指定してください');
  process.exit(2);
}
const AUTH = 'ESA ' + Buffer.from(`${SECRET}:${KEY}`).toString('base64');

const args = process.argv.slice(2);
const argOf = f => { const i = args.indexOf(f); return i >= 0 ? args[i + 1] : null; };
const DAYS = parseInt(argOf('--days') || '30', 10);
const CONFIRM = process.env.CONTRACT_TEST_CONFIRM === 'yes';

const results = [];
const sleep = ms => new Promise(r => setTimeout(r, ms));
let lastCall = 0;

// 全呼び出しを1.1秒以上空ける (RMS APIレート制限 1req/s 順守。§2.3)
async function call(name, url, { method = 'GET', body = null, headers = {}, note = '' } = {}) {
  const wait = lastCall + 1100 - Date.now();
  if (wait > 0) await sleep(wait);
  lastCall = Date.now();
  const t0 = Date.now();
  const entry = { name, method, url: url.replace(/api\.rms/, 'api.rms'), note, at: new Date().toISOString() };
  try {
    const res = await fetch(url, {
      method,
      headers: { Authorization: headers.authOverride || AUTH, 'Content-Type': 'application/json;charset=utf-8', ...headers },
      body: body ? JSON.stringify(body) : undefined,
    });
    entry.status = res.status;
    entry.ms = Date.now() - t0;
    entry.responseHeaders = Object.fromEntries([...res.headers.entries()].filter(([k]) =>
      /^(x-|retry-after|content-type)/i.test(k)));
    const ct = res.headers.get('content-type') || '';
    if (/json/.test(ct)) {
      entry.body = await res.json();
    } else {
      const buf = Buffer.from(await res.arrayBuffer());
      entry.bodyBytes = buf.length;
      entry.bodyHead = buf.slice(0, 200).toString('utf8');
    }
  } catch (e) {
    entry.error = String(e);
  }
  results.push(entry);
  console.log(`  [${entry.status ?? 'ERR'}] ${name} (${entry.ms ?? '-'}ms)${entry.error ? ' ' + entry.error : ''}`);
  return entry;
}

// 日付パラメータ: OSSクライアントは 'yyyy-MM-ddTHH:mm:ss' (タイムゾーン表記なし)。
// JST解釈かUTC解釈かは本テストの確認項目 (§2.4)。境界の問い合わせ日時と件数で判定する。
const fmt = d => {
  const p = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
};
const now = new Date();
const from = new Date(now.getTime() - DAYS * 86400000);
const range = `fromDate=${encodeURIComponent(fmt(from))}&toDate=${encodeURIComponent(fmt(now))}`;

console.log(`楽天 問い合わせ管理API 契約テスト (期間: 直近${DAYS}日, read-only${CONFIRM ? '+変更系許可' : ''})`);

// ── 1. 件数 ──
await call('count', `${BASE}/inquiries/count?${range}`);
await call('count (noMerchantReply=true)', `${BASE}/inquiries/count?${range}&noMerchantReply=true`,
  { note: '店舗未返信のみの件数。差分同期での利用可否確認' });

// ── 2. 一覧 + ページング ──
const list1 = await call('list page=1', `${BASE}/inquiries?${range}&limit=50&page=1`,
  { note: 'totalCount/totalPageCount/list[] の実形。limit最大値も別途確認' });
const totalPages = list1.body?.totalPageCount ?? 0;
if (totalPages > 1) {
  await call('list page=2', `${BASE}/inquiries?${range}&limit=50&page=2`, { note: 'ページ境界の重複/欠落確認用' });
}
await call('list limit=101 (上限探り)', `${BASE}/inquiries?${range}&limit=101&page=1`,
  { note: 'limit上限超過時のエラー形 or 黙って丸めるかを確認' });

// ── 3. 詳細 (最大3件) + reply.id 安定性 ──
const inquiries = list1.body?.list ?? [];
const targets = inquiries.slice(0, 3).map(i => i.inquiryNumber).filter(Boolean);
let firstDetail = null;
for (const n of targets) {
  const d = await call(`detail ${n}`, `${BASE}/inquiry/${encodeURIComponent(n)}`,
    { note: 'orderNumber/itemNumber/replies[].id/attachments の実形' });
  if (!firstDetail) firstDetail = d;
}
if (firstDetail?.body?.result?.inquiryNumber) {
  const n = firstDetail.body.result.inquiryNumber;
  const again = await call(`detail ${n} (再取得)`, `${BASE}/inquiry/${encodeURIComponent(n)}`,
    { note: 'replies[].id が再取得で同一か = 同期の外部ID安定性 (§2.4)' });
  const ids1 = (firstDetail.body.result.replies || []).map(r => r.id).join(',');
  const ids2 = (again.body?.result?.replies || []).map(r => r.id).join(',');
  results.push({ name: 'CHECK reply.id 安定性', stable: ids1 === ids2, ids1, ids2 });
  console.log(`  CHECK reply.id 安定性: ${ids1 === ids2 ? 'OK (同一)' : `NG (${ids1} vs ${ids2})`}`);
}

// ── 4. 添付ダウンロード (存在すれば1件) ──
const withAtt = inquiries.find(i => (i.attachments || []).length > 0);
if (withAtt) {
  const a = withAtt.attachments[0];
  await call('attachment GET', `${BASE}/attachment?path=${encodeURIComponent(a.path)}&label=${encodeURIComponent(a.label)}`,
    { note: 'バイト数のみ記録 (中身は保存しない)。保存期間・サイズ上限は別途確認' });
} else {
  results.push({ name: 'attachment GET', skipped: '期間内に添付付き問い合わせなし' });
}

// ── 5. エラー形 (--error-probes) ──
if (args.includes('--error-probes')) {
  await call('auth error (壊れたキー)', `${BASE}/inquiries/count?${range}`,
    { headers: { authOverride: 'ESA ' + Buffer.from('bad:bad').toString('base64') }, note: '401/403のレスポンス形' });
  await call('存在しない問い合わせ番号', `${BASE}/inquiry/NOT-EXIST-000`, { note: '404のレスポンス形' });
}

// ── 6. 変更系 (CONTRACT_TEST_CONFIRM=yes + 明示指定のみ) ──
for (const [flag, name, make] of [
  ['--send-reply', 'reply送信', n => ({ url: `${BASE}/inquiry/reply`, method: 'POST',
    // shopId は必須 (2026-08-17 実測: 欠落だと IE001 bad parameter)。inquiryNumber 先頭セグメント
    body: { inquiryNumber: n, shopId: Number(String(n).split('-')[0]), message: argOf('--message') || '契約テスト送信 (B-Faith社内テスト)' },
    note: '⚠️実返信。ReplyResultに返信IDが返るか = 二重送信照合の要 (§2.4)' })],
  ['--mark-read', '既読化', n => ({ url: `${BASE}/inquiries/read`, method: 'PATCH', body: { inquiryNumbers: [n] } })],
  ['--complete', '完了化', n => ({ url: `${BASE}/inquiries/complete`, method: 'PATCH', body: { inquiryNumbers: [n] } })],
  ['--incomplete', '未完了化', n => ({ url: `${BASE}/inquiries/incomplete`, method: 'PATCH', body: { inquiryNumbers: [n] } })],
]) {
  const n = argOf(flag);
  if (!n) continue;
  if (!CONFIRM) {
    console.error(`SKIP ${name}: 変更系は CONTRACT_TEST_CONFIRM=yes が必要です`);
    results.push({ name, skipped: 'CONTRACT_TEST_CONFIRM未設定' });
    continue;
  }
  const { url, method, body, note } = make(n);
  const r = await call(`${name} ${n}`, url, { method, body, note });
  if (flag === '--send-reply' && r.status === 200) {
    // 送信直後の詳細再取得で自返信を特定できるか (unknown照合の実現可否)
    await call(`detail ${n} (返信直後)`, `${BASE}/inquiry/${encodeURIComponent(n)}`,
      { note: '直前のReplyResultとreplies[]の対応を目視確認' });
  }
}

// ── 保存 ──
const outDir = path.join(process.cwd(), 'contract-test-results');
fs.mkdirSync(outDir, { recursive: true });
const file = path.join(outDir, `rakuten-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
fs.writeFileSync(file, JSON.stringify({ ranAt: new Date().toISOString(), days: DAYS, results }, null, 2));
console.log(`\n結果を保存: ${file}`);
console.log('※ 顧客情報を含みます。Gドライブ/リポジトリに置かないこと (契約テストノートには要点のみ転記)');
