// 🔗本文中のURLをリンクにする (linkify.js + 詳細画面の描画) のスモーク
//   2026-08-26 中原さん要望「本文のリンクがテキストのままなのでリンクになるようにしてほしい」
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-linkify.mjs
import fs from 'fs';
import path from 'path';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-linkify-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-linkify-'));
process.env.DATA_DIR = workDir;
process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED = 'true';

const { initInquiryHubDB, getDB } = await import('./db.js');
const { linkifyText, urlSafeCut, trimUrlTail } = await import('./linkify.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

// 事故メールの実物 (メルカリShops問い合わせ通知)
const MERCARI_URL = 'https://mercari-shops.com/seller/shops/wQ6RpHSAjon7n2bdu9mELD/inquiries/2JVis7W6Lh9thDUEHVXPsn?source=deeplink';

// ─── 1. linkifyText ───
console.log('1. linkifyText');
{
  const out = linkifyText(`以下のURLより、内容をご確認ください。\n${MERCARI_URL}\n▼注文情報`);
  check('URLが<a>になる', out.includes(`<a href="${MERCARI_URL}"`), out);
  check('新しいタブで開く + rel=noopener', out.includes('target="_blank"') && out.includes('rel="noopener noreferrer"'));
  check('クエリの & がエスケープされない生URLで残らない', !/href="[^"]*[^m]&[^a]/.test(out));
  check('URL以外の文字はそのまま', out.includes('以下のURLより、内容をご確認ください。') && out.includes('▼注文情報'));
  check('改行はそのまま残す (br化は呼び元の責務)', out.includes('\n'));

  check('URLが無ければ素通し (エスケープのみ)', linkifyText('ただの本文です') === 'ただの本文です');
  check('リンク無し', !linkifyText('お問い合わせありがとうございます').includes('<a '));

  // XSS: エスケープが効いていること / http・https 以外はリンクにしない
  const xss = linkifyText('<script>alert(1)</script> "quoted" & ampersand');
  check('HTMLエスケープが効く', xss.includes('&lt;script&gt;') && xss.includes('&quot;quoted&quot;') && xss.includes('&amp;'));
  check('javascript: はリンクにしない', !linkifyText('javascript:alert(1)').includes('<a '));
  check('file:// / ftp:// はリンクにしない',
    !linkifyText('file:///c:/secret ftp://x.jp/a').includes('<a '));
  const evil = linkifyText('https://x.jp/a"onmouseover="alert(1)');
  check('URL内の二重引用符で属性を抜けられない', !evil.includes('"onmouseover'), evil);

  // 末尾の句読点・閉じ括弧はリンクに含めない
  const jp = linkifyText('詳細は https://example.jp/a をご覧ください。');
  check('URLの後ろの日本語がリンクに混ざらない', jp.includes('>https://example.jp/a</a>') && jp.includes(' をご覧ください。'), jp);
  check('末尾の句点を含めない', linkifyText('https://example.jp/a。').includes('>https://example.jp/a</a>'));
  check('末尾の閉じ括弧を含めない', linkifyText('(https://example.jp/a)').includes('>https://example.jp/a</a>'));
  check('全角括弧に囲まれてもURLだけ拾う', linkifyText('（https://example.jp/a）').includes('>https://example.jp/a</a>'));
  check('パスの途中のピリオドは残す', linkifyText('https://example.jp/a.pdf ').includes('>https://example.jp/a.pdf</a>'));

  // 複数URL
  const two = linkifyText('A https://a.jp/1 B https://b.jp/2 C');
  check('複数URLをすべてリンクにする', (two.match(/<a /g) || []).length === 2 && two.includes('A ') && two.includes(' B ') && two.includes(' C'));

  check('trimUrlTail 単体', trimUrlTail('https://x.jp/a).') === 'https://x.jp/a');
  check('null/undefined でも落ちない', linkifyText(null) === '' && linkifyText(undefined) === '');
}

// ─── 2. urlSafeCut (折りたたみでURLを割らない) ───
console.log('2. urlSafeCut');
{
  const text = `前置き\n${MERCARI_URL}\n後ろ`;
  const urlStart = text.indexOf(MERCARI_URL);
  const mid = urlStart + 20;   // URLのど真ん中で切ろうとする
  check('URLの途中の切り位置はURL手前へ下がる', urlSafeCut(text, mid) === urlStart, String(urlSafeCut(text, mid)));
  check('URLの外の切り位置はそのまま', urlSafeCut(text, 3) === 3);
  check('URL終端ちょうどはそのまま', urlSafeCut(text, urlStart + MERCARI_URL.length) === urlStart + MERCARI_URL.length);
  // URLが先頭から始まる場合は前に下がれない → URLの直後で切る (headPartを空にしない)
  check('先頭がURLならURL直後で切る', urlSafeCut(MERCARI_URL + '\n後ろ', 20) === MERCARI_URL.length);
  check('URLが無ければそのまま', urlSafeCut('ただの本文', 3) === 3);
}

// ─── 3. 詳細画面の描画 ───
console.log('3. 詳細画面の描画');
{
  const express = (await import('express')).default;
  const routerModule = await import('./router.js');
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const srv = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${srv.address().port}/apps/inquiry-hub`;

  const shopId = db.prepare(`INSERT INTO shops (channel_type, shop_name, account_identifier)
    VALUES ('email','メール','info@b-faith.biz')`).run().lastInsertRowid;
  const mkInquiry = (extId, body) => {
    const id = db.prepare(`INSERT INTO inquiries
        (channel_type, shop_id, external_inquiry_id, customer_identifier, subject, received_at)
      VALUES ('email',?,?,'customer@gmail.com','件名','2026-08-23T10:08:00Z')`)
      .run(shopId, extId).lastInsertRowid;
    db.prepare(`INSERT INTO inquiry_messages
        (inquiry_id, external_message_id, sender_type, sender_name, message_body_text, is_incoming, received_at)
      VALUES (?,?, 'customer', 'メルカリShops', ?, 1, '2026-08-23T10:08:00Z')`).run(id, `m-${extId}`, body);
    return id;
  };

  // 事故メールの本文を模した長文 (折りたたみが効き、URLは畳みの内側に来る)
  const longBody = [
    'メルカリShopsをご利用いただきありがとうございます。',
    'お取引中の注文に関して、お客さまからの問い合わせを受け付けました。',
    '',
    '▼お客さまからのメッセージ',
    'すみません、返金と返送用封筒の送付は不要です。',
    '',
    '以下のURLより、内容をご確認ください。',
    '',
    '▼問い合わせページ',
    MERCARI_URL,
    '',
    '▼注文情報',
    '注文番号：order_2JVd4mTLTFvPJMoxMpKCGo',
    '商品名：ライム オイル 10ml 精油 アロマ オイル 天然100%',
    '商品価格：￥748',
    '',
    '※このメールアドレスは送信専用です。',
  ].join('\n');
  const idLong = mkInquiry('lk-long', longBody);
  const htmlLong = await (await fetch(`${base}/inquiries/${idLong}`)).text();
  check('長文でもURLが<a>になる', htmlLong.includes(`<a href="${MERCARI_URL}"`), '見つからず');
  check('リンクは1つだけ (折りたたみで割れていない)',
    (htmlLong.match(new RegExp(MERCARI_URL.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g')) || []).length === 2,
    String((htmlLong.match(new RegExp(MERCARI_URL.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g')) || []).length));
  check('折りたたみ (続きを表示) は従来どおり出る', htmlLong.includes('続きを表示'));

  // 短文 (折りたたみなし)
  const idShort = mkInquiry('lk-short', `ご確認ください ${MERCARI_URL}`);
  const htmlShort = await (await fetch(`${base}/inquiries/${idShort}`)).text();
  check('短文でもURLが<a>になる', htmlShort.includes(`<a href="${MERCARI_URL}"`));
  check('本文のHTMLは壊れていない (msg-body が閉じている)', /<div class="msg-body">[\s\S]*?<\/div>/.test(htmlShort));

  // 本文にHTMLらしき文字列があってもエスケープされる
  const idXss = mkInquiry('lk-xss', '<img src=x onerror=alert(1)> https://ok.example.jp/a');
  const htmlXss = await (await fetch(`${base}/inquiries/${idXss}`)).text();
  check('本文中のHTMLはエスケープされたまま',
    htmlXss.includes('&lt;img src=x onerror=alert(1)&gt;') && !htmlXss.includes('<img src=x onerror'));
  check('同じ本文のURLはリンクになる', htmlXss.includes('<a href="https://ok.example.jp/a"'));

  // 社内メモも同じ扱い
  db.prepare(`INSERT INTO internal_notes (inquiry_id, user_id, body) VALUES (?,?,?)`)
    .run(idShort, 'smoke', `参考: ${MERCARI_URL}`);
  const htmlNote = await (await fetch(`${base}/inquiries/${idShort}`)).text();
  check('社内メモのURLもリンクになる', (htmlNote.match(/<a href="https:\/\/mercari-shops\.com/g) || []).length >= 2);

  await new Promise(r => srv.close(r));
}

console.log(`\n結果: PASS ${passed} / FAIL ${failed}`);
try { db.close(); } catch { /* close済みは無視 */ }
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
