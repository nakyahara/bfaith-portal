// ✨AI書き換え (ai-rewrite.js + router /ai-rewrite) のスモーク
// 使い方: DATA_DIR=<空ディレクトリ> node apps/inquiry-hub/smoke-ai-rewrite.mjs
// OpenAI は fetch モックで代替 (実APIは叩かない・キー不要)
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-rewrite-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
process.env.DATA_DIR = fs.mkdtempSync(path.join(baseDir, 'smoke-rewrite-'));

const { initInquiryHubDB, getDB, toUtcIso } = await import('./db.js');
const { aiRewriteEnabled, rewriteReply, REWRITE_STYLES } = await import('./ai-rewrite.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

// ─── 1. モジュール単体 (fetchモック) ───
console.log('1. rewriteReply 単体');
{
  const ENV = { OPENAI_API_KEY: 'sk-test' };
  const okFetch = (captured = {}) => async (url, opts) => {
    captured.url = url; captured.body = JSON.parse(opts.body); captured.auth = opts.headers.Authorization;
    return { ok: true, status: 200, json: async () => ({ choices: [{ message: { content: 'お世話になっております。書き換え後の文面です。' } }] }) };
  };

  check('aiRewriteEnabled: キー無しは false / 有りは true',
    aiRewriteEnabled({}) === false && aiRewriteEnabled(ENV) === true);
  check('スタイル定義は3種 (polite/soft/concise)',
    Object.keys(REWRITE_STYLES).length === 3 && REWRITE_STYLES.polite && REWRITE_STYLES.soft && REWRITE_STYLES.concise);

  const cap = {};
  const r = await rewriteReply({ style: 'polite', text: '了解です。返金します。', inquiryContext: '返金してほしいです', env: ENV, fetchImpl: okFetch(cap) });
  check('書き換え結果を返す', r.text.includes('書き換え後') && r.model === 'gpt-5.6-luna');
  check('OpenAI Chat Completions へPOST', cap.url === 'https://api.openai.com/v1/chat/completions' && cap.auth === 'Bearer sk-test');
  check('モデル既定 gpt-5.6-luna / max_completion_tokens 使用 (GPT-5系は max_tokens 不可)',
    cap.body.model === 'gpt-5.6-luna' && cap.body.max_completion_tokens > 0 && cap.body.max_tokens === undefined);
  check('temperature を送らない (GPT-5系は既定のみ)', cap.body.temperature === undefined);
  check('プロンプトに下書きと問い合わせ文脈が入る',
    cap.body.messages[1].content.includes('返金します') && cap.body.messages[1].content.includes('返金してほしいです'));
  check('システムプロンプトで事実追加を禁止', cap.body.messages[0].content.includes('勝手に追加しない'));

  const cap2 = {};
  await rewriteReply({ style: 'concise', text: 'x', env: { ...ENV, OPENAI_REWRITE_MODEL: 'gpt-5.4-mini' }, fetchImpl: okFetch(cap2) });
  check('モデルはenvで差し替え可能', cap2.body.model === 'gpt-5.4-mini');

  // 拒否系
  const err = async (p) => { try { await rewriteReply(p); return null; } catch (e) { return e.message; } };
  check('不正スタイルは拒否', (await err({ style: 'yolo', text: 'x', env: ENV, fetchImpl: okFetch() }))?.includes('不正なスタイル'));
  check('空本文は拒否', (await err({ style: 'polite', text: '  ', env: ENV, fetchImpl: okFetch() }))?.includes('空'));
  check('長すぎる本文は拒否', (await err({ style: 'polite', text: 'あ'.repeat(5001), env: ENV, fetchImpl: okFetch() }))?.includes('長すぎ'));
  check('キー未設定は拒否', (await err({ style: 'polite', text: 'x', env: {}, fetchImpl: okFetch() }))?.includes('未設定'));

  // APIエラーの翻訳
  const errFetch = (status, message) => async () => ({ ok: false, status, json: async () => ({ error: { message } }) });
  check('401→キー確認の日本語', (await err({ style: 'polite', text: 'x', env: ENV, fetchImpl: errFetch(401, 'bad key') }))?.includes('OPENAI_API_KEY'));
  check('404→モデル名確認の日本語', (await err({ style: 'polite', text: 'x', env: ENV, fetchImpl: errFetch(404, 'model not found') }))?.includes('OPENAI_REWRITE_MODEL'));
  check('429→混雑/残高の日本語', (await err({ style: 'polite', text: 'x', env: ENV, fetchImpl: errFetch(429, 'rate limit') }))?.includes('混み合'));
  check('空応答は拒否', (await err({ style: 'polite', text: 'x', env: ENV, fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ choices: [{ message: { content: '' } }] }) }) }))?.includes('空の応答'));
}

// ─── 2. HTTP (UI表示ゲート + API) ───
console.log('2. HTTP');
{
  process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED = 'true';
  const routerModule = await import('./router.js');
  const shop = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','メール','info@b-faith.biz')").run().lastInsertRowid;
  const inq = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject, customer_identifier, internal_status, is_unread, received_at, conversation_rev)
    VALUES ('email', ?, 'rw1', '返金について', 'c@example.com', 'open', 1, ?, 1)`).run(shop, toUtcIso(Date.now())).lastInsertRowid;
  db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type, message_body_text, is_incoming, received_at)
    VALUES (?, 'rw-m1', 'customer', '返金してほしいです。', 1, ?)`).run(inq, toUtcIso(Date.now()));

  const app = express();
  app.use('/apps/inquiry-hub', express.json(), routerModule.default);
  const srv = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${srv.address().port}/apps/inquiry-hub`;
  const jp = (p, data) => fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) });

  // キー未設定: ボタンを出さない・APIは403
  delete process.env.OPENAI_API_KEY;
  const htmlOff = await (await fetch(`${base}/inquiries/${inq}`)).text();
  check('キー未設定: ✨ボタンを出さない (ダークローンチ)', !htmlOff.includes('id="rwRow"'));
  check('キー未設定: APIは403', (await jp(`/api/inquiries/${inq}/ai-rewrite`, { style: 'polite', text: 'x' })).status === 403);

  // キー設定後: ボタンが出る (envは起動後でも都度評価される設計)
  process.env.OPENAI_API_KEY = 'sk-test';
  const htmlOn = await (await fetch(`${base}/inquiries/${inq}`)).text();
  check('キー設定後: ✨ボタン3種+元に戻すが出る', htmlOn.includes('id="rwRow"')
    && htmlOn.includes('丁寧に') && htmlOn.includes('やわらかく') && htmlOn.includes('簡潔に') && htmlOn.includes('元に戻す'));
  check('クライアントJSが載る', htmlOn.includes("post('/ai-rewrite'"));

  // API 入力検証 (実fetchに到達する前に弾かれる系)
  check('API: 不正スタイルは400', (await jp(`/api/inquiries/${inq}/ai-rewrite`, { style: 'yolo', text: 'x' })).status === 400);
  check('API: 空本文は400', (await jp(`/api/inquiries/${inq}/ai-rewrite`, { style: 'polite', text: '' })).status === 400);
  check('API: 存在しない問い合わせは404', (await jp(`/api/inquiries/999999/ai-rewrite`, { style: 'polite', text: 'x' })).status === 404);
  delete process.env.OPENAI_API_KEY;

  srv.close();
}

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
