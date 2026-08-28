/**
 * extract.js (証憑の読み取り) のテスト。AIは fetch を差し替えて偽の応答で検証する (実APIは叩かない)
 * 実行: node apps/shohyo-links/tests/test-extract.mjs
 */
const { pickDate, pickAmount, extractByRules, normalizeAiResult, extractByAi, extractVoucher } = await import('../extract.js');

let failed = 0;
const check = (label, cond) => { console.log(`${cond ? 'OK ' : 'NG '} ${label}`); if (!cond) failed++; };

// 郵便局のネットショップ 領収書 (2026-08-28 中原さん提供) の pdf-parse 出力相当
const JP_POST = `領収書 2026年08月28日発行
B－Faith株式会社 様
¥60,000-
(内訳) 10%対象 0円 注文日 2026年08月17日
（内 消費税） 0円 オーダーＩＤ EC260817-646033523
8%対象 0円
非課税対象 60,000円
お支払内訳 クレジットカード 60,000円
商品明細 区分 数量 単価 金額
レターパックプラス（600円）（20部セット） 5 12,000円 60,000円
送料計 0円
日本郵便株式会社`;

let d = pickDate(JP_POST);
check('発行日より注文日を優先する', d?.value === '2026-08-17' && d.key === '注文日');
let a = pickAmount(JP_POST);
check('¥60,000- の額面を拾う (60,000円 の内訳と同額)', a?.value === 60000);
check('合計キーワード付きを優先', pickAmount('小計 1,000円\n消費税 100円\n合計 1,100円')?.value === 1100);
check('お支払金額 (全角数字)', pickAmount('お支払金額 ２，２００円')?.value === 2200);
check('日付が無ければ null', pickDate('金額 100円') === null);
check('存在しない日付は拾わない', pickDate('注文日 2026年02月30日') === null);
check('スラッシュ区切り', pickDate('ご利用日 2026/08/06')?.value === '2026-08-06');
check('「有効期限」の日付は飛ばして次の日付', pickDate('¥ 44,000 / 有効期限：2026年06月30日 / 2026年05月08日 / 見積書')?.value === '2026-05-08');
check('お支払期限より請求日を優先', pickDate('お支払期限：2026/06/30 請求日：2026/05/19')?.value === '2026-05-19');
const r = extractByRules(JP_POST);
check('extractByRules は日付+金額 (支払先は null)', r.doc_date === '2026-08-17' && r.amount === 60000 && r.vendor_name === null);

// AI応答の正規化
let n = normalizeAiResult({ doc_date: '2026-08-17', amount: '60000', vendor_name: ' 郵便局のネットショップ ', confidence: 1.5 });
check('AI応答: 文字列の金額・余白・範囲外confidence を正規化', n.doc_date === '2026-08-17' && n.amount === 60000 && n.vendor_name === '郵便局のネットショップ' && n.confidence === 1);
n = normalizeAiResult({ doc_date: '2026-13-01', amount: -5, vendor_name: 12 });
check('AI応答: 不正値は null', n.doc_date === null && n.amount === null && n.vendor_name === null);

// AI呼び出し (偽fetch)
let captured = null;
const fakeFetch = async (url, opts) => {
  captured = JSON.parse(opts.body);
  return { ok: true, json: async () => ({ choices: [{ message: { content: JSON.stringify({ doc_date: '2026-08-17', amount: 60000, vendor_name: '郵便局のネットショップ', confidence: 0.9 }) } }] }) };
};
const env = { OPENAI_API_KEY: 'k', SHOHYO_EXTRACT_MODEL: 'test-model' };
let ai = await extractByAi({ text: JP_POST, env, fetchImpl: fakeFetch });
check('テキストをAIに渡して JSON を受ける', ai.vendor_name === '郵便局のネットショップ' && ai.model === 'test-model' && captured.response_format.type === 'json_object' && captured.messages[1].content.includes('注文日'));
ai = await extractByAi({ imageBuffer: Buffer.from([0xFF, 0xD8, 0xFF]), mime: 'image/jpeg', env, fetchImpl: fakeFetch });
check('画像は data URL で渡す', Array.isArray(captured.messages[1].content) && captured.messages[1].content[1].image_url.url.startsWith('data:image/jpeg;base64,'));
check('鍵が無ければ null (AIを呼ばない)', (await extractByAi({ text: 'x', env: {}, fetchImpl: fakeFetch })) === null);
ai = await extractByAi({ pdfBuffer: Buffer.from('%PDF-1.4 scan'), fileName: 'doc (3).pdf', env, fetchImpl: fakeFetch });
check('文字のないPDFは file として渡す', Array.isArray(captured.messages[1].content) && captured.messages[1].content[1].type === 'file' && captured.messages[1].content[1].file.file_data.startsWith('data:application/pdf;base64,') && captured.messages[1].content[1].file.filename === 'doc (3).pdf');
const badFetch = async () => ({ ok: false, status: 429, json: async () => ({ error: { message: 'rate' } }) });
let threw = false;
try { await extractByAi({ text: 'x', env, fetchImpl: badFetch }); } catch (e) { threw = e.message.startsWith('openai_429'); }
check('APIエラーは openai_<status> で throw', threw);

// 入口 (PDFテキスト化は pdf-parse に依存するので、ここでは画像経路とAI失敗の耐性を見る)
const img = await extractVoucher(Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), 'jpg', { env, fetchImpl: fakeFetch });
check('画像は AI で読み source=ai', img.source === 'ai' && img.amount === 60000 && img.vendor_name === '郵便局のネットショップ');
const noKey = await extractVoucher(Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), 'jpg', { env: {}, fetchImpl: fakeFetch });
check('鍵なし画像は何も取れない (source=none・理由が notes に残る)', noKey.source === 'none' && noKey.amount === null && noKey.notes.some(n => n.includes('OPENAI_API_KEY')));
// 文字のないPDF (pdf-parse が読めない中身) → PDFごと AI へ
captured = null;
const scanPdf = await extractVoucher(Buffer.from('%PDF-1.4 scanned image only'), 'pdf', { env, fetchImpl: fakeFetch, fileName: 'doc (3).pdf' });
check('文字のないPDFは PDFごと AI に渡して読む', scanPdf.source === 'ai' && scanPdf.amount === 60000 && captured?.messages[1].content[1].type === 'file' && scanPdf.notes.some(n => n.includes('埋め込まれていません')));
const aiFail = await extractVoucher(Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), 'jpg', { env, fetchImpl: badFetch });
check('AI失敗でも落ちない (ai_error を持つ)', aiFail.source === 'none' && aiFail.ai_error?.startsWith('openai_429'));

console.log(failed ? `\n${failed}件NG` : '\n全件パス');
process.exit(failed ? 1 : 0);
