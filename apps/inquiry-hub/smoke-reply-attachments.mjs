// inquiry-hub 返信の送信用添付スモーク (2026-08-20 スタッフ要望「PDFなどを添付できるように」)
// 使い方: DATA_DIR=<空ディレクトリ> node apps/inquiry-hub/smoke-reply-attachments.mjs
// ⚠️ DATA_DIR 内の inquiry-hub.db を作り直すため本番 DATA_DIR で実行禁止 (smoke.mjs と同じガード)
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import express from 'express';
import { initInquiryHubDB, getDB, toUtcIso } from './db.js';
import { saveReplyAttachment, listPendingAttachments, deletePendingAttachment, pruneOrphanAttachments,
  loadJobAttachments, MAX_FILE_BYTES, MAX_FILES_PER_REPLY } from './reply-attachments.js';
import { createReplyJob, runOutboxPass, SendRejectedError } from './outbox.js';
import { createGmailAdapter } from './sync/adapters/gmail.js';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です。専用の空ディレクトリを指定してください');
  process.exit(2);
}
const dbPath = path.join(process.env.DATA_DIR, 'inquiry-hub.db');
const markerPath = path.join(process.env.DATA_DIR, '.inquiry-hub-smoke-db');
if (fs.existsSync(dbPath) && !fs.existsSync(markerPath)) {
  console.error('FATAL: DATA_DIR に既存の inquiry-hub.db があります (smoke 生成マーカーなし)。中断します');
  process.exit(2);
}
for (const suffix of ['', '-wal', '-shm']) { try { fs.rmSync(dbPath + suffix); } catch {} }

initInquiryHubDB();
const db = getDB();
fs.mkdirSync(process.env.DATA_DIR, { recursive: true });
fs.writeFileSync(markerPath, `created by apps/inquiry-hub/smoke-reply-attachments.mjs at ${new Date().toISOString()}\n`);

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};
const throws = (name, fn, msgPart) => {
  try { fn(); check(name, false, '例外なし'); }
  catch (e) { check(name, !msgPart || String(e.message).includes(msgPart), `期待(${msgPart}) 実際(${e.message})`); }
};

const PDF = Buffer.concat([Buffer.from('%PDF-1.4\n1 0 obj\n'), Buffer.alloc(64, 0x20)]);
const PNG = Buffer.concat([Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]), Buffer.alloc(32, 1)]);
const TXT = Buffer.from('これはテキストです');

const shopEmail = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','メール窓口','info@b-faith.biz')").run().lastInsertRowid;
const shopRk = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('rakuten','楽天店','rk-shop')").run().lastInsertRowid;
const mkInquiry = (shopId, channel, extId) => db.prepare(`INSERT INTO inquiries
    (channel_type, shop_id, external_inquiry_id, customer_name, customer_identifier, subject, conversation_rev, received_at)
  VALUES (?,?,?,?,?,?,0,?)`).run(channel, shopId, extId, '顧客', 'customer@example.com', '納品書がほしい', toUtcIso(Date.now())).lastInsertRowid;

// ─── 1. 保存の検証 (形式・サイズ・件数) ───
console.log('1. 保存検証');
const inqM = mkInquiry(shopEmail, 'email', 'th-att-1');
const inqRk = mkInquiry(shopRk, 'rakuten', 'rk-att-1');
{
  const a1 = saveReplyAttachment({ inquiryId: inqM, fileName: '納品書_2026.pdf', buffer: PDF, uploadedBy: 'staff-a' });
  check('PDF保存 (中身の先頭バイトで形式確定)', a1.id > 0 && a1.contentType === 'application/pdf' && a1.fileSize === PDF.length);
  const a2 = saveReplyAttachment({ inquiryId: inqM, fileName: 'photo.png', buffer: PNG, uploadedBy: 'staff-a' });
  check('PNG保存', a2.contentType === 'image/png');
  throws('偽装拡張子 (中身テキストの.pdf) は拒否', () => saveReplyAttachment({ inquiryId: inqM, fileName: 'nise.pdf', buffer: TXT, uploadedBy: 'x' }), '添付できません');
  throws('空ファイルは拒否', () => saveReplyAttachment({ inquiryId: inqM, fileName: 'a.pdf', buffer: Buffer.alloc(0), uploadedBy: 'x' }), '空');
  throws('サイズ超過は拒否', () => saveReplyAttachment({ inquiryId: inqM, fileName: 'big.pdf',
    buffer: Buffer.concat([Buffer.from('%PDF-'), Buffer.alloc(MAX_FILE_BYTES, 0x20)]), uploadedBy: 'x' }), '大きすぎます');
  throws('メール以外のチャネルは拒否', () => saveReplyAttachment({ inquiryId: inqRk, fileName: 'a.pdf', buffer: PDF, uploadedBy: 'x' }), 'メール返信のみ');
  saveReplyAttachment({ inquiryId: inqM, fileName: '3つ目.pdf', buffer: PDF, uploadedBy: 'x' });
  throws(`${MAX_FILES_PER_REPLY + 1}つ目は拒否`, () => saveReplyAttachment({ inquiryId: inqM, fileName: '4つ目.pdf', buffer: PDF, uploadedBy: 'x' }), 'つまで');
  const list = listPendingAttachments(inqM);
  check('未紐付け一覧 (3件・本体は含まない)', list.length === 3 && list.every(a => a.id && a.name && a.size && !('body' in a)));
  deletePendingAttachment(inqM, list[2].id);
  check('未紐付けの削除', listPendingAttachments(inqM).length === 2);
  throws('存在しないIDの削除は拒否', () => deletePendingAttachment(inqM, 99999), '削除できません');
  // 24時間放置の掃除
  const orphan = saveReplyAttachment({ inquiryId: inqM, fileName: '放置.pdf', buffer: PDF, uploadedBy: 'x' });
  db.prepare("UPDATE outbox_attachments SET created_at = strftime('%Y-%m-%dT%H:%M:%SZ','now','-25 hours') WHERE id = ?").run(orphan.id);
  pruneOrphanAttachments();
  check('未紐付け24時間で掃除', !listPendingAttachments(inqM).some(a => a.id === orphan.id) && listPendingAttachments(inqM).length === 2);
}

// ─── 2. ジョブへの紐付け (createReplyJob) ───
console.log('2. ジョブ紐付け');
{
  const atts = listPendingAttachments(inqM);
  throws('楽天チャネルに添付は拒否', () => createReplyJob({ inquiryId: inqRk, channelType: 'rakuten', bodyText: 'x',
    createdBy: 'u', clientOperationId: 'op-rk-att', baseConversationRev: 0, attachmentIds: [atts[0].id] }), 'メール返信のみ');
  const r = createReplyJob({ inquiryId: inqM, channelType: 'email', bodyText: '納品書を添付します',
    createdBy: 'staff-a', clientOperationId: 'op-att-1', baseConversationRev: 0, attachmentIds: atts.map(a => a.id) });
  const job = db.prepare('SELECT * FROM outbox_replies WHERE id = ?').get(r.id);
  const recorded = JSON.parse(job.attachments_json);
  check('ジョブ作成で紐付け+attachments_json記録', r.created && recorded.length === 2
    && recorded[0].name === '納品書_2026.pdf'
    && db.prepare('SELECT COUNT(*) c FROM outbox_attachments WHERE outbox_id = ?').get(r.id).c === 2);
  check('紐付け後は未紐付け一覧から消える', listPendingAttachments(inqM).length === 0);
  throws('紐付け済み添付の削除は拒否', () => deletePendingAttachment(inqM, atts[0].id), '削除できません');
  const loaded = loadJobAttachments(r.id);
  check('ワーカー用の実体読み出し (Buffer一致)', loaded.length === 2
    && Buffer.compare(loaded[0].buffer, PDF) === 0 && Buffer.compare(loaded[1].buffer, PNG) === 0);
  // 使用済みIDを別ジョブへ → 拒否 (ジョブごとロールバックしてジョブも残らない)
  const inqM2 = mkInquiry(shopEmail, 'email', 'th-att-2');
  const before = db.prepare('SELECT COUNT(*) c FROM outbox_replies').get().c;
  throws('使用済み添付IDの再利用は拒否', () => createReplyJob({ inquiryId: inqM2, channelType: 'email', bodyText: 'x',
    createdBy: 'u', clientOperationId: 'op-att-2', baseConversationRev: 0, attachmentIds: [atts[0].id] }), '添付が見つかりません');
  check('拒否時はジョブも作られない (同一トランザクション)', db.prepare('SELECT COUNT(*) c FROM outbox_replies').get().c === before);
}

// ─── 3. 送信ワーカー (実体の受け渡し・欠落時の安全側) ───
console.log('3. 送信ワーカー');
{
  let got = null;
  const adapter = { sendReply: async ({ attachments, attachmentsJson }) => { got = { attachments, attachmentsJson }; return { externalReplyId: 'sent-att-1' }; } };
  const r = await runOutboxPass({ email: adapter }, { now: Date.now() });
  check('アダプターに実体 (Buffer) が渡る', r.results[0]?.outcome === 'sent'
    && got.attachments.length === 2 && Buffer.compare(got.attachments[0].buffer, PDF) === 0
    && got.attachments[0].fileName === '納品書_2026.pdf' && got.attachments[0].contentType === 'application/pdf');

  // 記録に添付ありなのに実体が欠けている → 未送信確定 (failed)。アダプターは呼ばれない
  const inqGone = mkInquiry(shopEmail, 'email', 'th-att-gone');
  const aG = saveReplyAttachment({ inquiryId: inqGone, fileName: 'g.pdf', buffer: PDF, uploadedBy: 'x' });
  const rG = createReplyJob({ inquiryId: inqGone, channelType: 'email', bodyText: 'x',
    createdBy: 'u', clientOperationId: 'op-att-gone', baseConversationRev: 0, attachmentIds: [aG.id] });
  db.prepare('DELETE FROM outbox_attachments WHERE id = ?').run(aG.id);
  let called = 0;
  const r2 = await runOutboxPass({ email: { sendReply: async () => { called++; return {}; } } }, { now: Date.now() });
  const jobG = db.prepare('SELECT * FROM outbox_replies WHERE id = ?').get(rG.id);
  check('実体欠落は failed (未送信確定・アダプター未呼び出し)', r2.results[0]?.outcome === 'failed'
    && called === 0 && jobG.status === 'failed' && jobG.error_message.includes('添付の実体'));
}

// ─── 4. Gmail multipart/mixed 組み立て ───
console.log('4. Gmail MIME');
{
  const CRED = { clientId: 'c', clientSecret: 's', refreshToken: 'r', sleepMs: 0 };
  const threadMeta = { id: 'g1', messages: [{ id: 'a1', payload: { headers: [{ name: 'Message-ID', value: '<orig-1@mail>' }] } }] };
  let sendBody = null;
  const f = async (url, opts) => {
    if (url.includes('oauth2.googleapis.com')) return { ok: true, status: 200, json: async () => ({ access_token: 'tok', expires_in: 3600 }) };
    if (url.includes('/threads/')) return { ok: true, status: 200, json: async () => threadMeta };
    if (url.includes('messages/send')) { sendBody = JSON.parse(opts.body); return { ok: true, status: 200, json: async () => ({ id: 'sent-mp', threadId: 'g1' }) }; }
    return { ok: true, status: 200, json: async () => ({}) };
  };
  const ad = createGmailAdapter({ ...CRED, fetchImpl: f, sendMode: 'live' });
  const atts = [
    { fileName: '納品書_2026.pdf', contentType: 'application/pdf', buffer: PDF },
    { fileName: 'photo.png', contentType: 'image/png', buffer: PNG },
  ];
  const sent = await ad.sendReply({
    inquiry: { external_inquiry_id: 'g1', customer_identifier: 'c@example.com', subject: '納品書' },
    bodyText: '添付します。\nご確認ください', attachments: atts,
  });
  const mime = Buffer.from(sendBody.raw.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
  const boundary = mime.match(/boundary="([^"]+)"/)?.[1];
  check('multipart/mixed + boundary', sent.externalReplyId === 'sent-mp'
    && mime.includes('Content-Type: multipart/mixed') && !!boundary);
  const parts = mime.split(`--${boundary}`);
  check('本文+添付2つの3パート+終端', parts.length === 5 && parts[4].startsWith('--'));
  check('本文パートがbase64で復元できる', Buffer.from(
    (parts[1].split('\r\n\r\n')[1] || '').replace(/\s/g, ''), 'base64').toString('utf8') === '添付します。\nご確認ください');
  check('PDFパート: Content-Disposition attachment + 日本語ファイル名 (RFC2231)',
    parts[2].includes('Content-Disposition: attachment') && parts[2].includes("filename*=UTF-8''%E7%B4%8D%E5%93%81%E6%9B%B8"));
  check('PDFパートの実体が一致', Buffer.compare(Buffer.from(
    (parts[2].split('\r\n\r\n')[1] || '').replace(/\s/g, ''), 'base64'), PDF) === 0);
  check('PNGパートの実体が一致', Buffer.compare(Buffer.from(
    (parts[3].split('\r\n\r\n')[1] || '').replace(/\s/g, ''), 'base64'), PNG) === 0);
  check('In-Reply-To等のスレッドヘッダは従来どおり', mime.includes('In-Reply-To: <orig-1@mail>') && sendBody.threadId === 'g1');

  // 添付なしは従来の単一パートのまま (回帰)
  let plainBody = null;
  const f2 = async (url, opts) => {
    if (url.includes('oauth2.googleapis.com')) return { ok: true, status: 200, json: async () => ({ access_token: 'tok', expires_in: 3600 }) };
    if (url.includes('/threads/')) return { ok: true, status: 200, json: async () => threadMeta };
    if (url.includes('messages/send')) { plainBody = JSON.parse(opts.body); return { ok: true, status: 200, json: async () => ({ id: 'sent-pl' }) }; }
    return { ok: true, status: 200, json: async () => ({}) };
  };
  await createGmailAdapter({ ...CRED, fetchImpl: f2, sendMode: 'live' }).sendReply({
    inquiry: { external_inquiry_id: 'g1', customer_identifier: 'c@example.com', subject: 'a' }, bodyText: 'テスト本文' });
  const mimePlain = Buffer.from(plainBody.raw.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
  check('添付なしは text/plain 単一パート (回帰)', mimePlain.includes('Content-Type: text/plain')
    && !mimePlain.includes('multipart')
    && Buffer.from(mimePlain.split('\r\n\r\n')[1].replace(/\r\n/g, ''), 'base64').toString('utf8') === 'テスト本文');
}

// ─── 5. HTTP (アップロードAPI・画面・/reply連携) ───
console.log('5. HTTP');
{
  process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED = 'true';
  const routerModule = await import('./router.js');
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const srv = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${srv.address().port}/apps/inquiry-hub`;
  const upload = (inqId, name, buf) => fetch(`${base}/api/inquiries/${inqId}/reply-attachments`, {
    method: 'POST', headers: { 'Content-Type': 'application/octet-stream', 'X-File-Name': encodeURIComponent(name) }, body: buf,
  });
  const jp = (p, data) => fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) });

  const inqH = mkInquiry(shopEmail, 'email', 'th-att-http');
  const html = await (await fetch(`${base}/inquiries/${inqH}`)).text();
  check('メール詳細に📎添付ボタン+ファイル入力', html.includes('id="attBtn"') && html.includes('id="attFile"') && html.includes('id="attList"'));
  check('一覧はtextContent構築 (ファイル名をinnerHTMLに入れない)', html.includes('name.textContent') && !html.includes('attList.innerHTML'));
  const htmlRk = await (await fetch(`${base}/inquiries/${inqRk}`)).text();
  check('楽天詳細には添付ボタンを出さない', !htmlRk.includes('id="attBtn"'));

  const up1 = await upload(inqH, '請求書_8月分.pdf', PDF);
  const up1j = await up1.json();
  check('バイナリアップロード成功 (日本語ファイル名)', up1.status === 200 && up1j.ok && up1j.fileName === '請求書_8月分.pdf' && up1j.id > 0);
  const upBad = await upload(inqH, 'nise.pdf', TXT);
  check('偽装ファイルは400', upBad.status === 400 && String((await upBad.json()).error).includes('添付できません'));
  const upRk = await upload(inqRk, 'a.pdf', PDF);
  check('楽天へのアップロードは400', upRk.status === 400);
  const wrongCt = await fetch(`${base}/api/inquiries/${inqH}/status`, {
    method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: PDF });
  check('他のAPIはoctet-streamを415で拒否 (JSON必須のまま)', wrongCt.status === 415);

  const html2 = await (await fetch(`${base}/inquiries/${inqH}`)).text();
  check('リロードでアップロード済みが復元される (var ATT)', html2.includes('請求書_8月分.pdf'));

  const del = await jp(`/api/inquiries/${inqH}/reply-attachments/${up1j.id}/delete`, {});
  check('未紐付けの取り消しAPI', del.status === 200 && (await (await fetch(`${base}/inquiries/${inqH}`)).text()).includes('請求書_8月分.pdf') === false);

  // /reply に attachmentIds を渡してジョブ作成
  const up2 = await (await upload(inqH, '納品書.pdf', PDF)).json();
  const opId = crypto.randomUUID();
  const rep = await jp(`/api/inquiries/${inqH}/reply`, { body: '納品書を添付します', clientOperationId: opId, baseConversationRev: 0, attachmentIds: [up2.id] });
  const repJ = await rep.json();
  const jobRow = db.prepare('SELECT * FROM outbox_replies WHERE client_operation_id = ?').get(opId);
  check('/reply が添付付きジョブを作成', rep.status === 200 && repJ.ok
    && JSON.parse(jobRow.attachments_json).length === 1
    && db.prepare('SELECT outbox_id FROM outbox_attachments WHERE id = ?').get(up2.id).outbox_id === jobRow.id);
  const badIds = await jp(`/api/inquiries/${inqH}/reply`, { body: 'x', clientOperationId: crypto.randomUUID(), baseConversationRev: 0, attachmentIds: ['a'] });
  check('attachmentIds の型検証 (400)', badIds.status === 400);
  const html3 = await (await fetch(`${base}/inquiries/${inqH}`)).text();
  check('送信ジョブ履歴に📎件数表示', html3.includes('📎1件'));

  srv.close();
  delete process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED;
}

// DATA_DIR外に書いていないことの確認 (他smokeと同様の防御)
check('DBは一時ディレクトリのみに作成', fs.existsSync(dbPath));

console.log(`\n${passed} PASS / ${failed} FAIL`);
// process.exit() は稼働中のasyncハンドルとぶつかりWindowsでlibuv abortする (exitCodeで自然終了)
process.exitCode = failed ? 1 : 0;
