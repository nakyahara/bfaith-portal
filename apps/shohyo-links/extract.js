/**
 * shohyo-links — 証憑 (PDF/画像) から 日付・金額・支払先 を読み取る (証憑自動添付 ②「読む」)
 *
 * 方針: **AIは目、プログラムは手** (2026-08-28 中原さん合意)。ここは「目」。
 * ここで読んだ値はあくまで突合の入力で、貼るかどうかの判断は matcher.js のルールが行う。
 * 読み違えても「一致しない → 貼られない」側に倒れるだけなので、AIを使ってよい。
 *
 * 手順:
 *   1. PDF は pdf-parse でテキスト化。キーワード付きの日付・金額 (注文日/合計 等) をルールで拾う (AI不要・無料)
 *   2. ルールで足りない (支払先・金額が取れない) とき、OPENAI_API_KEY があれば OpenAI に JSON で読ませる
 *      (PDFはテキストを渡す。画像 (写真のレシート) は画像そのものを渡す)
 *   3. 何も取れなければ空のまま (人が「直す」で入れる)
 *
 * env: OPENAI_API_KEY (inquiry-hub と共用) / SHOHYO_EXTRACT_MODEL (既定 gpt-5.6-luna)
 */
import { isValidDate } from './matcher.js';

const OPENAI_URL = 'https://api.openai.com/v1/chat/completions';
const DEFAULT_MODEL = 'gpt-5.6-luna';
const REQUEST_TIMEOUT_MS = 40000;
const MAX_TEXT_CHARS = 6000;

export function extractEnabled(env = process.env) {
  return !!env.OPENAI_API_KEY;
}

// ---- 1. ルール抽出 (PDFテキスト) ----

const DATE_RE = /(20\d{2})[年\/\-.]\s*(\d{1,2})[月\/\-.]\s*(\d{1,2})\s*日?/g;
// 取引日として優先するキーワード (発行日は最後の手段。発行日は取引日より後になりがち)
const DATE_KEYS = ['注文日', '取引日', '利用日', 'ご利用日', '購入日', '決済日', '支払日', 'お支払日', '売上日', '請求日', '日付', '発行日', '領収日'];
// 「金額」単体は表の見出しにも出るので使わない (商品行の単価を拾ってしまう)
const AMOUNT_KEYS = ['合計金額', 'お支払金額', 'お支払い金額', '支払金額', 'ご請求金額', '請求金額', '税込合計', '合計', '総額', 'お支払額', '領収金額'];

const ymd = (y, m, d) => `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;

/** テキストから日付を拾う。キーワードの直後にある日付を優先し、無ければ最初の日付 */
export function pickDate(text) {
  const t = String(text || '');
  for (const key of DATE_KEYS) {
    const re = new RegExp(key + '[^\\d]{0,12}(20\\d{2})[年\\/\\-.]\\s*(\\d{1,2})[月\\/\\-.]\\s*(\\d{1,2})');
    const m = t.match(re);
    if (m) { const v = ymd(m[1], m[2], m[3]); if (isValidDate(v)) return { value: v, key }; }
  }
  // キーワード無しの日付: 「期限」(有効期限・支払期限) の直後にある日付は取引日ではないので飛ばす
  for (const m of t.matchAll(DATE_RE)) {
    if (/期限|まで/.test(t.slice(Math.max(0, m.index - 10), m.index))) continue;
    const v = ymd(m[1], m[2], m[3]);
    if (isValidDate(v)) return { value: v, key: '' };
  }
  return null;
}

/** テキストから金額を拾う。¥60,000- / 60,000円 / 合計 60,000 の形。キーワード付きを優先、無ければ最大の ¥ 表記 */
export function pickAmount(text) {
  const t = String(text || '').replace(/[０-９，]/g, c => String.fromCharCode(c.charCodeAt(0) - 0xFEE0));
  const num = (s) => Number(String(s).replace(/,/g, ''));
  for (const key of AMOUNT_KEYS) {
    const re = new RegExp(key + '[^\\d¥￥]{0,12}[¥￥]?\\s*(\\d{1,3}(?:,\\d{3})+|\\d+)\\s*(?:円|-)?');
    const m = t.match(re);
    if (m && num(m[1]) > 0) return { value: num(m[1]), key };
  }
  // ¥60,000- のような独立表記 (領収書の額面)
  const yen = [...t.matchAll(/[¥￥]\s*(\d{1,3}(?:,\d{3})+|\d+)\s*-?/g)].map(m => num(m[1])).filter(n => n > 0);
  if (yen.length) return { value: Math.max(...yen), key: '¥' };
  return null;
}

/** PDF のテキストからルールで読む (AI不要)。取れなかった項目は null */
export function extractByRules(text) {
  const d = pickDate(text);
  const a = pickAmount(text);
  return { doc_date: d?.value || null, amount: a?.value || null, vendor_name: null, how: { date: d?.key ?? null, amount: a?.key ?? null } };
}

// ---- 2. AI 抽出 ----

const SYSTEM_PROMPT = `あなたは経理担当者です。渡された領収書・請求書から次の3項目を読み取り、JSONだけを返してください。
{"doc_date": "YYYY-MM-DD または null", "amount": 整数(円) または null, "vendor_name": "発行者(お店・会社)の短い名前 または null", "confidence": 0〜1}

守ること:
- doc_date は「取引日・注文日・利用日・購入日」。それが無ければ発行日。宛名の会社ではなく、その書類の日付
- amount は支払総額 (税込・実際に払った額)。小計や税額ではない。値引き後・ポイント利用後の請求額
- vendor_name は発行者 (お金を受け取った側)。宛名 (B-Faith株式会社 等) ではない。カード明細に出る屋号に近い表記が望ましい
- 読めない項目は null。推測で埋めない`;

async function callOpenAI({ apiKey, model, userContent, fetchImpl }) {
  const res = await fetchImpl(OPENAI_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify({
      model,
      messages: [{ role: 'system', content: SYSTEM_PROMPT }, { role: 'user', content: userContent }],
      response_format: { type: 'json_object' },
      max_completion_tokens: 300, // GPT-5系は temperature 指定不可
    }),
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`openai_${res.status}: ${body?.error?.message || ''}`.slice(0, 200));
  const content = body?.choices?.[0]?.message?.content || '{}';
  try { return JSON.parse(content); } catch { throw new Error('openai_bad_json'); }
}

/** AIの返答を安全な形に正規化する (型・範囲を信用しない) */
export function normalizeAiResult(j) {
  const out = { doc_date: null, amount: null, vendor_name: null, confidence: 0 };
  if (j && isValidDate(j.doc_date)) out.doc_date = j.doc_date;
  const n = Number(j?.amount);
  if (Number.isSafeInteger(n) && n > 0 && n <= 1_000_000_000) out.amount = n;
  if (typeof j?.vendor_name === 'string' && j.vendor_name.trim()) out.vendor_name = j.vendor_name.trim().slice(0, 200);
  const c = Number(j?.confidence);
  out.confidence = Number.isFinite(c) ? Math.max(0, Math.min(1, c)) : 0;
  return out;
}

export async function extractByAi({ text = '', imageBuffer = null, mime = '', env = process.env, fetchImpl = fetch }) {
  const apiKey = env.OPENAI_API_KEY;
  if (!apiKey) return null;
  const model = env.SHOHYO_EXTRACT_MODEL || DEFAULT_MODEL;
  let userContent;
  if (imageBuffer) {
    userContent = [
      { type: 'text', text: 'この領収書の画像から3項目を読み取ってください。' },
      { type: 'image_url', image_url: { url: `data:${mime || 'image/jpeg'};base64,${imageBuffer.toString('base64')}` } },
    ];
  } else {
    userContent = `【領収書のテキスト】\n${String(text).slice(0, MAX_TEXT_CHARS)}`;
  }
  return { ...normalizeAiResult(await callOpenAI({ apiKey, model, userContent, fetchImpl })), model };
}

// ---- 3. 入口 ----

async function pdfText(buffer) {
  try {
    // v1.1.1 は package root を ESM import すると自己テストが走り test/data を読みに行く → lib を直接読む
    const mod = await import('pdf-parse/lib/pdf-parse.js');
    const fn = mod.default || mod;
    const r = await fn(buffer);
    return String(r?.text || '');
  } catch (e) {
    console.warn('[shohyo-extract] pdf-parse failed:', e.message);
    return '';
  }
}

/**
 * 証憑から 日付・金額・支払先 を読む。
 * @param {Buffer} buffer
 * @param {'pdf'|'jpg'|'png'} ext
 * @returns {{ doc_date, amount, vendor_name, source: 'rules'|'ai'|'rules+ai'|'none', text_chars, ai?: {confidence, model} }}
 */
export async function extractVoucher(buffer, ext, { env = process.env, fetchImpl = fetch } = {}) {
  const out = { doc_date: null, amount: null, vendor_name: null, source: 'none', text_chars: 0 };
  let text = '';
  if (ext === 'pdf') {
    text = await pdfText(buffer);
    out.text_chars = text.length;
    const r = extractByRules(text);
    out.doc_date = r.doc_date; out.amount = r.amount;
    if (r.doc_date || r.amount) out.source = 'rules';
  }
  // 支払先はルールでは取れないので、鍵があればAIに読ませる (画像は必ずAI)
  const needAi = !out.vendor_name || !out.amount || !out.doc_date;
  if (needAi && extractEnabled(env)) {
    try {
      const ai = ext === 'pdf'
        ? (text.trim() ? await extractByAi({ text, env, fetchImpl }) : null)
        : await extractByAi({ imageBuffer: buffer, mime: ext === 'png' ? 'image/png' : 'image/jpeg', env, fetchImpl });
      if (ai) {
        // ルールで取れた日付・金額はAIより優先 (決定的なので)。支払先はAIから
        out.doc_date = out.doc_date || ai.doc_date;
        out.amount = out.amount || ai.amount;
        out.vendor_name = ai.vendor_name;
        out.ai = { confidence: ai.confidence, model: ai.model };
        out.source = out.source === 'rules' ? 'rules+ai' : 'ai';
      }
    } catch (e) {
      console.warn('[shohyo-extract] ai failed:', e.message);
      out.ai_error = String(e.message).slice(0, 200);
    }
  }
  return out;
}
