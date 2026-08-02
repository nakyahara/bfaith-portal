// 📎添付ファイル表示 (mime.js + attachments.js + router /attachments/:id + アダプターfetchAttachment) のスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-attachments.mjs
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-att-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-att-'));
process.env.DATA_DIR = workDir;
const baseDbExistedAtStart = fs.existsSync(path.join(baseDir, 'inquiry-hub.db'));

// Gmailアダプターの生成に必要な env (実通信はしない。fetchImpl を差し替える)
process.env.INQUIRY_GMAIL_CLIENT_ID = 'test-client';
process.env.INQUIRY_GMAIL_CLIENT_SECRET = 'test-secret';
process.env.INQUIRY_GMAIL_REFRESH_TOKEN = 'test-refresh';

const { initInquiryHubDB, getDB } = await import('./db.js');
const mime = await import('./mime.js');
const { getAttachmentContext, contentDispositionValue } = await import('./attachments.js');
const { createGmailAdapter } = await import('./sync/adapters/gmail.js');
const { createRakutenAdapter } = await import('./sync/adapters/rakuten.js');
const { createYahooAdapter } = await import('./sync/adapters/yahoo.js');
const { mapTopicDetail } = await import('./sync/adapters/yahoo.js');
const { runSync } = await import('./sync/engine.js');
const routerModule = await import('./router.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

// ─── 1. MIME判定 (inline表示の安全性) ───
console.log('1. MIME判定');
{
  check('拡張子から画像を確定', mime.resolveContentType('写真.JPG', 'application/octet-stream') === 'image/jpeg');
  check('PDFはinline可', mime.isInlineSafe(mime.resolveContentType('見積.pdf', null)));
  check('SVGはinlineさせない (octet-stream)', mime.resolveContentType('logo.svg', 'image/svg+xml') === 'application/octet-stream');
  check('HTMLもinlineさせない', mime.resolveContentType('a.html', 'text/html') === 'application/octet-stream');
  check('未知拡張子+未知申告はoctet-stream', mime.resolveContentType('a.xyz', 'application/x-weird') === 'application/octet-stream');
  check('画像判定', mime.isImage('image/png') && !mime.isImage('application/pdf'));
  check('サイズ表記', mime.fmtBytes(2048) === '2KB' && mime.fmtBytes(0) === '' && mime.fmtBytes(3 * 1048576) === '3.0MB');
  check('拡張子→型 (Yahoo fileExt用)', mime.contentTypeFromExt('PNG') === 'image/png' && mime.contentTypeFromExt('.pdf') === 'application/pdf');

  // 中身 (先頭バイト) と拡張子が食い違うものは inline させない
  const realPng = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 0]);
  const fakePng = Buffer.from('<html><script>alert(1)</script></html>');
  const realPdf = Buffer.from('%PDF-1.7 test');
  check('magic bytes 判定 (PNG/JPEG/PDF)', mime.sniffContentType(realPng) === 'image/png'
    && mime.sniffContentType(Buffer.from([0xff, 0xd8, 0xff, 0xe0])) === 'image/jpeg'
    && mime.sniffContentType(realPdf) === 'application/pdf');
  check('中身が一致する画像はそのまま配信', mime.contentTypeForServing('a.png', 'image/png', realPng) === 'image/png');
  check('拡張子を偽ったファイルはoctet-streamへ落とす',
    mime.contentTypeForServing('a.png', 'image/png', fakePng) === 'application/octet-stream');
  check('中身が一致するPDFはinline可', mime.isInlineSafe(mime.contentTypeForServing('a.pdf', null, realPdf)));
  check('判定不能 (短すぎる) もoctet-stream', mime.contentTypeForServing('a.png', 'image/png', Buffer.from([1])) === 'application/octet-stream');

  const cd = contentDispositionValue('請求書 2026.pdf', true);
  check('Content-Disposition: inline + RFC5987', cd.startsWith('inline;') && cd.includes("filename*=UTF-8''") && cd.includes('%E8%AB%8B'), cd);
  check('Content-Disposition: 非ASCIIはASCII名にも落とす', /filename="[\x20-\x7e]+"/.test(cd), cd);
  const cdInj = contentDispositionValue('a"\r\nX-Evil: 1.png', false);
  check('ヘッダーインジェクションを持ち込まない', !/[\r\n]/.test(cdInj) && cdInj.startsWith('attachment;'), cdInj);
}

// ─── 2. アダプター: Gmail (メッセージ再取得 → attachmentId → 本体) ───
console.log('2. Gmail添付取得');
{
  const png = Buffer.from('89504e470d0a1a0a0000000d49484452', 'hex');
  const b64url = png.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  const calls = [];
  const fetchImpl = async (url) => {
    calls.push(String(url));
    if (String(url).includes('oauth2')) return new Response(JSON.stringify({ access_token: 't', expires_in: 3600 }), { status: 200 });
    if (String(url).includes('/attachments/')) return new Response(JSON.stringify({ size: png.length, data: b64url }), { status: 200 });
    return new Response(JSON.stringify({
      id: 'msg1',
      payload: { mimeType: 'multipart/mixed', parts: [
        { partId: '0', mimeType: 'text/plain', body: { data: '' } },
        { partId: '1', filename: '写真.png', mimeType: 'image/png', body: { attachmentId: 'att-abc', size: png.length } },
        { partId: '2', filename: '写真.png', mimeType: 'image/png', body: { attachmentId: 'att-other', size: 999 } },
      ] },
    }), { status: 200 });
  };
  const ad = createGmailAdapter({ clientId: 'a', clientSecret: 'b', refreshToken: 'c', fetchImpl, sleepMs: 0 });
  // partId 指定 (同期時に保存した外部ID) があれば、それでpartを特定する
  const byPart = await ad.fetchAttachment({ externalAttachmentId: 'part:2', externalMessageId: 'msg1', fileName: '写真.png' });
  check('partIdで正しいpartを引く (同名・別サイズでも取り違えない)',
    Buffer.compare(byPart.buffer, png) === 0 && calls.some(u => u.includes('att-other')));
  calls.length = 0;
  const got = await ad.fetchAttachment({ externalMessageId: 'msg1', fileName: '写真.png', fileSize: png.length });
  check('本体を取得できる', Buffer.compare(got.buffer, png) === 0);
  check('Content-Typeはpartのmime', got.contentType === 'image/png');
  check('同名添付はサイズ一致を優先 (att-abc)', calls.some(u => u.includes('att-abc')) && !calls.some(u => u.includes('att-other')));

  let notFound = null;
  try { await ad.fetchAttachment({ externalMessageId: 'msg1', fileName: '無い.png' }); } catch (e) { notFound = e; }
  check('見つからない添付はthrow', notFound !== null && /見つかりません/.test(notFound.message));

  let synErr = null;
  try { await ad.fetchAttachment({ externalMessageId: 'syn:abc', fileName: 'x.png' }); } catch (e) { synErr = e; }
  check('synthetic messageId は取得不可と分かる文言', synErr !== null && /再同期/.test(synErr.message));

  let tooBig = null;
  try { await ad.fetchAttachment({ externalMessageId: 'msg1', fileName: '写真.png', fileSize: png.length, maxBytes: 4 }); } catch (e) { tooBig = e; }
  check('maxBytes超過はthrow', tooBig !== null && /大きすぎ/.test(tooBig.message));
}

// ─── 3. アダプター: 楽天 (path+label) / Yahoo (objectKey) ───
console.log('3. 楽天・Yahoo添付取得');
{
  const bin = Buffer.from('hello-image');
  let lastUrl = null;
  const fetchImpl = async (url) => {
    lastUrl = String(url);
    return new Response(bin, { status: 200, headers: { 'Content-Type': 'image/jpeg', 'Content-Length': String(bin.length) } });
  };
  const rk = createRakutenAdapter({ transport: 'warehouse', warehouseUrl: 'https://wh.example.com', serviceToken: 't',
    cfClientId: 'ci', cfClientSecret: 'cs', fetchImpl, sleepMs: 0 });
  const g1 = await rk.fetchAttachment({ externalAttachmentId: 'attach/2026/abc.jpg', fileName: '不具合.jpg' });
  check('楽天: miniPC passthrough を叩く', lastUrl.startsWith('https://wh.example.com/service-api/rakuten-rms/inquiry-attachment?path='), lastUrl);
  check('楽天: path/label がURLエンコードされる', lastUrl.includes('path=attach%2F2026%2Fabc.jpg') && lastUrl.includes('label=%E4%B8%8D%E5%85%B7%E5%90%88.jpg'), lastUrl);
  check('楽天: 本体とContent-Type', Buffer.compare(g1.buffer, bin) === 0 && g1.contentType === 'image/jpeg');

  const rkDirect = createRakutenAdapter({ transport: 'direct', serviceSecret: 's', licenseKey: 'l', fetchImpl, sleepMs: 0 });
  await rkDirect.fetchAttachment({ externalAttachmentId: 'p', fileName: 'f.png' });
  check('楽天(direct): RMS直URL', lastUrl.startsWith('https://api.rms.rakuten.co.jp/es/1.0/inquirymng-api/attachment?path='), lastUrl);

  let rkSyn = null;
  try { await rk.fetchAttachment({ externalAttachmentId: 'syn:zzz', fileName: 'x.png' }); } catch (e) { rkSyn = e; }
  check('楽天: 取得キーが無い添付はthrow', rkSyn !== null && /取得キー/.test(rkSyn.message));

  // Content-Length を偽って申告しても、実バイト数で打ち切る (Codexレビュー High-1)
  const liar = async () => new Response(Buffer.alloc(1024), { status: 200,
    headers: { 'Content-Type': 'image/png', 'Content-Length': '10' } });
  const rkLiar = createRakutenAdapter({ transport: 'direct', serviceSecret: 's', licenseKey: 'l', fetchImpl: liar, sleepMs: 0 });
  let liarErr = null;
  try { await rkLiar.fetchAttachment({ externalAttachmentId: 'p', fileName: 'f.png', maxBytes: 100 }); } catch (e) { liarErr = e; }
  check('Content-Length過小申告でも実バイト数で打ち切る', liarErr !== null && /大きすぎ/.test(liarErr.message));

  const yh = createYahooAdapter({ proxyUrl: 'https://vps.example.com', proxySecret: 'ps', fetchImpl, sleepMs: 0 });
  const g2 = await yh.fetchAttachment({ externalAttachmentId: 'obj/key/123', fileName: '画像.png' });
  check('Yahoo: VPSプロキシの externalTalkFile を叩く',
    lastUrl.startsWith('https://vps.example.com/yahoo/externalTalkFile?key=') && lastUrl.includes('obj%2Fkey%2F123'), lastUrl);
  check('Yahoo: 本体を取得できる', Buffer.compare(g2.buffer, bin) === 0);

  let big = null;
  try { await yh.fetchAttachment({ externalAttachmentId: 'k', fileName: 'x.png', maxBytes: 3 }); } catch (e) { big = e; }
  check('Yahoo: Content-Lengthで事前に上限判定', big !== null && /大きすぎ/.test(big.message));
}

// ─── 3.5 Gmail: walkPayload が partId を外部IDにする ───
console.log('3.5 Gmail添付の外部ID');
{
  const { walkPayload } = await import('./sync/adapters/gmail.js');
  const out = walkPayload({ mimeType: 'multipart/mixed', parts: [
    { partId: '1', filename: 'a.png', mimeType: 'image/png', body: { attachmentId: 'x', size: 10 } },
    { partId: '2', filename: 'a.png', mimeType: 'image/png', body: { attachmentId: 'y', size: 10 } },
  ] });
  check('同名・同サイズでも partId で別IDになる (1件に潰れない)',
    out.attachments.length === 2 && out.attachments[0].externalAttachmentId === 'part:1'
    && out.attachments[1].externalAttachmentId === 'part:2', JSON.stringify(out.attachments));
}

// ─── 4. Yahoo detail → objectKey のマッピング + 同期時の昇格 ───
console.log('4. objectKeyのマッピングと昇格');
{
  const detail = {
    topic: { title: '商品について', isComplete: false },
    messages: [{ messageId: 1, postUserType: 'user', postdate: '1785000000', body: '写真です',
      fileList: [{ fileName: 'photo.png', objectKey: 'obj-1', fileExt: 'png', fileSize: 1234 }] }],
  };
  const mapped = mapTopicDetail('topic-1', detail);
  const att = mapped.messages[0].attachments[0];
  check('objectKey が外部IDになる', att.externalAttachmentId === 'obj-1');
  check('fileExt から Content-Type を補完', att.contentType === 'image/png');
  check('fileSize を拾う', att.fileSize === 1234);

  // 旧実装 (objectKey を拾えず synthetic ID で保存済み) の行が再同期で実IDへ昇格するか。
  // 同期エンジン経由 (本番と同じ経路) で検証する
  db.prepare(`INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('yahoo','Yストア','seller1')`).run();
  const shop = db.prepare("SELECT * FROM shops WHERE channel_type='yahoo'").get();
  db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject, received_at)
    VALUES ('yahoo', ?, 'topic-1', '商品について', '2026-08-01T00:00:00Z')`).run(shop.id);
  const inqId = db.prepare("SELECT id FROM inquiries WHERE external_inquiry_id='topic-1'").get().id;
  db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type, message_body_text, is_incoming, received_at)
    VALUES (?, 'm:1', 'customer', '写真です', 1, '2026-08-01T00:00:00Z')`).run(inqId);
  const msgId = db.prepare('SELECT id FROM inquiry_messages WHERE inquiry_id = ?').get(inqId).id;
  // 旧同期が残した synthetic 行 (objectKey を拾えていなかった時代のデータ)
  db.prepare(`INSERT INTO inquiry_attachments (inquiry_message_id, external_attachment_id, file_name)
    VALUES (?, 'syn:legacyhash', 'photo.png')`).run(msgId);

  const mockAdapter = { channelType: 'yahoo', async fetchNew() { return { inquiries: [mapped] }; } };
  const T = Date.parse('2026-08-02T00:00:00Z');
  await runSync(shop.id, mockAdapter, { now: T });
  const atts = db.prepare('SELECT * FROM inquiry_attachments WHERE inquiry_message_id = ?').all(msgId);
  check('syn: 行が実objectKeyへ昇格し重複しない', atts.length === 1 && atts[0].external_attachment_id === 'obj-1', JSON.stringify(atts));
  check('content_type/file_size が後から埋まる', atts[0].content_type === 'image/png' && atts[0].file_size === 1234);

  // 2回目 (既に実IDで入っている状態) でも増えない
  await runSync(shop.id, mockAdapter, { now: T + 60000 });
  check('再同期しても添付は1件のまま',
    db.prepare('SELECT COUNT(*) AS c FROM inquiry_attachments WHERE inquiry_message_id = ?').get(msgId).c === 1);
}

// ─── 5. HTTP 配信 (Content-Type / ダウンロード強制 / 失敗時) ───
console.log('5. HTTP配信');
{
  const attRow = db.prepare('SELECT * FROM inquiry_attachments LIMIT 1').get();
  const ctx = getAttachmentContext(attRow.id);
  check('コンテキストにチャネル・メッセージIDが乗る', ctx.channel_type === 'yahoo' && ctx.external_message_id === 'm:1');
  check('存在しないIDはnull', getAttachmentContext(999999) === null);

  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${server.address().port}/apps/inquiry-hub`;

  const r404 = await fetch(`${base}/attachments/999999`);
  check('存在しない添付は404', r404.status === 404);

  // Yahoo transport env が無い状態 → 502 + 日本語エラー (画面が壊れない)
  const rErr = await fetch(`${base}/attachments/${attRow.id}`);
  const errText = await rErr.text();
  check('取得できないときは502', rErr.status === 502 && errText.includes('添付を取得できませんでした'));
  check('内部事情 (env・上流URL) は画面に出さずエラーIDのみ',
    errText.includes('エラーID') && !errText.includes('env') && !errText.includes('http'), errText);

  const detailHtml = await (await fetch(`${base}/inquiries/${db.prepare('SELECT id FROM inquiries LIMIT 1').get().id}`)).text();
  check('詳細画面に画像サムネイル (img + lazy)', detailHtml.includes(`/apps/inquiry-hub/attachments/${attRow.id}`) && detailHtml.includes('loading="lazy"'));
  check('取得失敗時の文言を仕込んである', detailHtml.includes('取得できませんでした'));
  check('保存リンク (?download=1)', detailHtml.includes(`/apps/inquiry-hub/attachments/${attRow.id}?download=1`));

  server.close();
}

check('DBは一時サブディレクトリのみに作成',
  fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && fs.existsSync(path.join(baseDir, 'inquiry-hub.db')) === baseDbExistedAtStart);

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
