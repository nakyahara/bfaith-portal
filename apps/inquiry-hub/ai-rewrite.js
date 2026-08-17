/**
 * 返信文のAI書き換え (2026-08-17 スタッフ要望: 「丁寧に/やわらかく/簡潔に」ボタンでその場で整える)
 *
 * - OpenAI Chat Completions を直接 fetch で叩く (SDK依存を増やさない)
 * - env OPENAI_API_KEY があるときだけ有効 (ダークローンチ。UI側もボタンを出さない)
 * - モデルは env OPENAI_REWRITE_MODEL で差し替え可能 (既定 gpt-5.6-luna = 2026-08時点の
 *   最軽量・最安世代。品質に不満が出たら env を gpt-5.4-mini 等へ変えるだけ)
 * - 顧客の問い合わせ本文を文脈として渡す (OpenAI APIの入力は既定で学習に使われない)
 */

const OPENAI_URL = 'https://api.openai.com/v1/chat/completions';
const DEFAULT_MODEL = 'gpt-5.6-luna';
const MAX_INPUT_CHARS = 5000;      // 下書きの上限 (コスト暴走とAPI上限の防御)
const MAX_CONTEXT_CHARS = 1200;    // 問い合わせ文脈の上限
const MAX_THREAD_CHARS = 4000;     // 下書き生成に渡す会話履歴の上限
const REQUEST_TIMEOUT_MS = 30000;

/** AIが埋められなかった箇所のマーカー。画面はこれを数えて「要確認」警告を出す */
export const PLACEHOLDER_RE = /【要確認[:：][^】]*】/g;

export const REWRITE_STYLES = {
  polite:  { label: '丁寧に',     instruction: 'きちんとした敬語のビジネスメール文に整えてください。誠実で礼儀正しい印象にします。' },
  soft:    { label: 'やわらかく', instruction: 'かしこまりすぎない、感じが良く親しみやすい表現に書き換えてください。丁寧さは保ちます。' },
  concise: { label: '簡潔に',     instruction: '丁寧さを保ちながら、要点がすぐ伝わる簡潔な文にまとめてください。冗長な前置きや重複を削ります。' },
};

export function aiRewriteEnabled(env = process.env) {
  return !!env.OPENAI_API_KEY;
}

const SYSTEM_PROMPT = `あなたはECショップのカスタマーサポート返信文を仕上げる編集者です。
スタッフが書いた返信の下書きを、指示されたスタイルで書き換えてください。

守ること:
- 内容・事実・約束は変えない。書かれていない対応・補償・期日を勝手に追加しない
- お客様の名前・注文番号・商品名などの固有情報はそのまま残す
- 日本語のビジネスメールとして自然な文にする (宛名や署名が含まれていればそれも自然に整える)
- 出力は書き換え後の本文のみ。前置き・説明・引用符・コードブロックは一切付けない`;

/**
 * 返信下書きを指定スタイルで書き換える。
 * @param {object} p { style, text, inquiryContext?, env?, fetchImpl? }
 * @returns {Promise<{ text: string, model: string }>}
 * @throws Error (message はそのまま画面に出せる日本語)
 */
export async function rewriteReply({ style, text, inquiryContext = '', env = process.env, fetchImpl = fetch }) {
  const st = REWRITE_STYLES[style];
  if (!st) throw new Error(`不正なスタイルです: ${style}`);
  const draft = String(text || '').trim();
  if (!draft) throw new Error('本文が空です');
  if (draft.length > MAX_INPUT_CHARS) throw new Error(`本文が長すぎます (${MAX_INPUT_CHARS}文字まで)`);
  const apiKey = env.OPENAI_API_KEY;
  if (!apiKey) throw new Error('AI書き換えは未設定です (env OPENAI_API_KEY)');
  const model = env.OPENAI_REWRITE_MODEL || DEFAULT_MODEL;

  const context = String(inquiryContext || '').slice(0, MAX_CONTEXT_CHARS);
  const userPrompt = `${context ? `【お客様からの問い合わせ (参考文脈)】\n${context}\n\n` : ''}【スタッフの返信下書き】\n${draft}\n\n【指示: ${st.label}】\n${st.instruction}`;

  const out = await callOpenAI({ apiKey, model, system: SYSTEM_PROMPT, user: userPrompt, fetchImpl, label: 'AI書き換え' });
  return { text: out, model };
}

/** OpenAI Chat Completions 呼び出しの共通処理 (書き換え・下書き生成で共用)。
 * エラーは運用者が対処できる日本語に翻訳して throw する */
async function callOpenAI({ apiKey, model, system, user, fetchImpl, label, maxTokens = 2000 }) {
  let res;
  try {
    res = await fetchImpl(OPENAI_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
      body: JSON.stringify({
        model,
        messages: [
          { role: 'system', content: system },
          { role: 'user', content: user },
        ],
        // GPT-5系は temperature 指定不可 (既定のみ)・max_tokens ではなく max_completion_tokens
        max_completion_tokens: maxTokens,
      }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
  } catch (err) {
    const timedOut = err?.name === 'TimeoutError' || err?.name === 'AbortError';
    throw new Error(timedOut ? `${label}がタイムアウトしました。もう一度お試しください` : `AI接続エラー: ${String(err?.message || err).slice(0, 120)}`);
  }

  const j = await res.json().catch(() => null);
  if (!res.ok) {
    // エラー本文からはOpenAIのmessageだけ拾う (キー誤り・モデル名誤り・残高切れが典型)
    const msg = j?.error?.message || `HTTP ${res.status}`;
    if (res.status === 401) throw new Error(`${label}の認証に失敗しました (OPENAI_API_KEY を確認してください)`);
    if (res.status === 404) throw new Error(`AIモデルが見つかりません (OPENAI_REWRITE_MODEL='${model}' を確認してください)`);
    if (res.status === 429) throw new Error('AIが混み合っています (残高不足の可能性もあります)。少し待って再試行してください');
    throw new Error(`${label}に失敗しました: ${String(msg).slice(0, 200)}`);
  }

  const out = String(j?.choices?.[0]?.message?.content || '').trim();
  if (!out) throw new Error('AIから空の応答が返りました。もう一度お試しください');
  return out;
}

// ─── 返信の下書き生成 (2026-08-17 第1段階: 社内ナレッジを参照して下書く) ───

const DRAFT_SYSTEM_PROMPT = `あなたはECショップのカスタマーサポート担当者です。
お客様からの問い合わせに対する返信の「下書き」を作成してください。この下書きは人間のスタッフが
確認・修正してから送信します。

【最重要】事実を推測で書かないこと:
- 回答の根拠は、渡された「社内Q&A」「返信テンプレート」「注文情報」だけです
- 根拠がない事柄 (在庫の入荷時期、個別の配送状況、例外的な対応の可否、金額、日付など) は
  絶対に推測で書かず、その箇所を 【要確認: 何を確認すべきか】 の形で残してください
  例: 「商品は【要確認: 入荷予定日】に入荷予定です」「返金は【要確認: 返金可否と金額】となります」
- 「おそらく」「通常は」などで曖昧にごまかさず、分からないものは必ず【要確認:】で明示する
- 社内Q&Aに答えがある場合は、その内容に忠実に書いてください (勝手に条件を足さない)

【書き方】
- 日本語のビジネスメールとして自然な、丁寧で誠実な文面にする
- お客様の名前が分かる場合は冒頭で使う。注文番号・商品名は正確に転記する
- 返信テンプレートが渡されている場合は、その文体・言い回しに寄せる
- 出力は返信本文のみ。前置き・説明・引用符・コードブロックは付けない`;

/**
 * 問い合わせへの返信下書きをAIに作らせる。
 * @param {object} p { inquiry, messages, knowledgeText, env?, fetchImpl? }
 *   inquiry: { customer_name, order_number, product_name, subject, channel_type }
 *   messages: [{ is_incoming, sender_name, message_body_text }] 古い順
 * @returns {Promise<{ text, model, placeholders: string[] }>}
 */
export async function draftReply({ inquiry = {}, messages = [], knowledgeText = '', env = process.env, fetchImpl = fetch }) {
  const apiKey = env.OPENAI_API_KEY;
  if (!apiKey) throw new Error('AI下書きは未設定です (env OPENAI_API_KEY)');
  const model = env.OPENAI_REWRITE_MODEL || DEFAULT_MODEL;

  // 会話履歴 (新しい方を優先して上限まで)。長すぎる履歴はコストと精度の両方を損なう
  const thread = [];
  let used = 0;
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i];
    const line = `${m.is_incoming ? 'お客様' : '当店'}: ${String(m.message_body_text || '').replace(/\s+/g, ' ').trim()}`;
    if (!line.trim() || line.length + used > MAX_THREAD_CHARS) continue;
    thread.unshift(line);
    used += line.length;
  }
  if (!thread.length) throw new Error('この問い合わせには本文がないため下書きできません');

  const facts = [
    inquiry.customer_name ? `お客様のお名前: ${inquiry.customer_name}` : '',
    inquiry.order_number ? `注文番号: ${inquiry.order_number}` : '',
    inquiry.product_name ? `商品名: ${inquiry.product_name}` : '',
    inquiry.subject ? `件名: ${inquiry.subject}` : '',
  ].filter(Boolean).join('\n');

  const userPrompt = [
    facts ? `【この問い合わせの情報 (確定事実)】\n${facts}` : '',
    knowledgeText || '【社内Q&A】\n該当なし (この問い合わせに関係する社内の回答例は見つかりませんでした)',
    `【お客様とのやり取り (古い順)】\n${thread.join('\n')}`,
    '上記に対する返信の下書きを書いてください。根拠のない事柄は【要確認: ○○】で残すこと。',
  ].filter(Boolean).join('\n\n');

  const text = await callOpenAI({ apiKey, model, system: DRAFT_SYSTEM_PROMPT, user: userPrompt, fetchImpl, label: 'AI下書き', maxTokens: 2500 });
  const placeholders = [...new Set(text.match(PLACEHOLDER_RE) || [])];
  return { text, model, placeholders };
}
