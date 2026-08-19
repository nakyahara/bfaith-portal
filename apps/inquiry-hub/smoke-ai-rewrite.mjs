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

// ─── 1b. ナレッジ検索 (社内Q&A/テンプレから関連するものを選ぶ) ───
console.log('1b. knowledge.js');
{
  const { extractKeywords, findRelevantKnowledge, formatKnowledgeForPrompt, MAX_QA } = await import('./knowledge.js');

  check('キーワード抽出: 助詞・定型句を落とす',
    !extractKeywords('お世話になっております。よろしくお願いします。').includes('お世話'));
  const kw = extractKeywords('ひまし油の抽出方法について教えてください');
  check('キーワード抽出: 名詞が拾える', kw.includes('ひまし') || kw.includes('抽出方法') || kw.includes('抽出'));

  db.prepare(`INSERT INTO qa_entries (category, title, question, answer) VALUES
    ('商品について', 'ひまし油の抽出方法', '低温圧搾ですか?', '高温圧搾です。')`).run();
  db.prepare(`INSERT INTO qa_entries (category, title, question, answer) VALUES
    ('配送について', '配送日数の目安', '何日で届きますか', '発送から2〜5日です。')`).run();
  db.prepare(`INSERT INTO qa_entries (category, title, question, answer, is_published) VALUES
    ('内部', 'ひまし油の原価', '原価は?', '非公開', 0)`).run();
  db.prepare(`INSERT INTO reply_templates (category, template_name, template_body, keywords) VALUES
    ('商品説明', 'ひまし油の説明テンプレ', 'ひまし油は〜です。', 'ひまし油 抽出')`).run();

  const found = findRelevantKnowledge({ subject: 'ひまし油について', body: 'ひまし油の抽出方法を教えてください' });
  check('関連Q&Aを拾う', found.qa.some(q => q.title === 'ひまし油の抽出方法'), JSON.stringify(found.qa.map(q => q.title)));
  check('無関係なQ&A (配送) は拾わない', !found.qa.some(q => q.title === '配送日数の目安'));
  check('非公開Q&Aは除外', !found.qa.some(q => q.title === 'ひまし油の原価'));
  check('関連テンプレも拾う', found.templates.some(t => t.name === 'ひまし油の説明テンプレ'));
  check('スコア順に並ぶ', found.qa.length <= MAX_QA && (found.qa.length < 2 || found.qa[0].score >= found.qa[1].score));

  const none = findRelevantKnowledge({ subject: '', body: 'zzz' });
  check('該当なしなら空 (捏造材料を渡さない)', none.qa.length === 0 && none.templates.length === 0);
  check('プロンプト整形: Q&Aとテンプレの見出しが入る', (() => {
    const s = formatKnowledgeForPrompt(found);
    return s.includes('社内Q&A') && s.includes('高温圧搾') && s.includes('返信テンプレート');
  })());
  check('プロンプト整形: 空なら空文字', formatKnowledgeForPrompt({ qa: [], templates: [] }) === '');
}

// ─── 1c. draftReply (下書き生成) ───
console.log('1c. draftReply');
{
  const { draftReply, PLACEHOLDER_RE } = await import('./ai-rewrite.js');
  const ENV = { OPENAI_API_KEY: 'sk-test' };
  const mk = (content, captured = {}) => async (url, opts) => {
    captured.body = JSON.parse(opts.body);
    return { ok: true, status: 200, json: async () => ({ choices: [{ message: { content } }] }) };
  };
  const msgs = [
    { is_incoming: 1, message_body_text: '在庫はいつ入りますか?' },
    { is_incoming: 0, message_body_text: '確認いたします。' },
  ];

  const cap = {};
  const r = await draftReply({
    inquiry: { customer_name: '山田太郎', order_number: 'ORD-1', product_name: 'ひまし油', subject: '在庫について' },
    messages: msgs, knowledgeText: '【社内Q&A】\n1. 在庫\n   A: 週次で入荷',
    env: ENV, fetchImpl: mk('山田太郎様\n\n入荷は【要確認: 次回入荷予定日】です。\n【要確認: 数量】をご用意できます。', cap),
  });
  check('下書きを返す', r.text.includes('山田太郎様') && r.model === 'gpt-5.6-luna');
  check('【要確認:】を抽出 (重複排除)', r.placeholders.length === 2
    && r.placeholders.includes('【要確認: 次回入荷予定日】'), JSON.stringify(r.placeholders));
  check('プロンプトに確定事実 (名前/注文番号/商品) が入る',
    ['山田太郎', 'ORD-1', 'ひまし油'].every(s => cap.body.messages[1].content.includes(s)));
  check('プロンプトに会話履歴が古い順で入る', (() => {
    const c = cap.body.messages[1].content;
    return c.indexOf('在庫はいつ入りますか') < c.indexOf('確認いたします');
  })());
  check('プロンプトに社内Q&Aが入る', cap.body.messages[1].content.includes('週次で入荷'));
  check('システムプロンプトで推測禁止+要確認を指示',
    cap.body.messages[0].content.includes('推測で書かず') && cap.body.messages[0].content.includes('【要確認'));
  check('GPT-5系制約を踏襲 (temperature無し/max_completion_tokens)',
    cap.body.temperature === undefined && cap.body.max_completion_tokens > 0);

  const cap2 = {};
  await draftReply({ inquiry: {}, messages: msgs, knowledgeText: '', env: ENV, fetchImpl: mk('x', cap2) });
  check('ナレッジ無しは「該当なし」と明示 (捏造防止)', cap2.body.messages[1].content.includes('該当なし'));

  const err = async (p) => { try { await draftReply(p); return null; } catch (e) { return e.message; } };
  check('本文なしは拒否', (await err({ inquiry: {}, messages: [], env: ENV, fetchImpl: mk('x') }))?.includes('本文がない'));
  check('キー未設定は拒否', (await err({ inquiry: {}, messages: msgs, env: {}, fetchImpl: mk('x') }))?.includes('未設定'));
  check('PLACEHOLDER_RE が全角コロンにも効く', '【要確認：日付】'.match(PLACEHOLDER_RE)?.length === 1);
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
  check('キー未設定: 下書きボタンも出さない', !htmlOff.includes('id="draftBtn"'));
  check('キー未設定: APIは403', (await jp(`/api/inquiries/${inq}/ai-rewrite`, { style: 'polite', text: 'x' })).status === 403);
  check('キー未設定: 下書きAPIも403', (await jp(`/api/inquiries/${inq}/ai-draft`, {})).status === 403);

  // キー設定後: ボタンが出る (envは起動後でも都度評価される設計)
  process.env.OPENAI_API_KEY = 'sk-test';
  const htmlOn = await (await fetch(`${base}/inquiries/${inq}`)).text();
  check('キー設定後: ✨ボタン3種+元に戻すが出る', htmlOn.includes('id="rwRow"')
    && htmlOn.includes('丁寧に') && htmlOn.includes('やわらかく') && htmlOn.includes('簡潔に') && htmlOn.includes('元に戻す'));
  check('クライアントJSが載る', htmlOn.includes("post('/ai-rewrite'"));
  check('キー設定後: ✨AIで下書きボタン+要確認警告枠が出る',
    htmlOn.includes('id="draftBtn"') && htmlOn.includes('id="draftWarn"') && htmlOn.includes("post('/ai-draft'"));
  check('送信前に【要確認】残存チェックが入る', htmlOn.includes('未確認の箇所が'));
  check('警告表示は textContent 構築 (AI出力をinnerHTMLに入れない)',
    htmlOn.includes('warn.textContent') && !htmlOn.includes('warn.innerHTML'));
  check('下書きAPI: 存在しない問い合わせは404', (await jp('/api/inquiries/999999/ai-draft', {})).status === 404);

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
