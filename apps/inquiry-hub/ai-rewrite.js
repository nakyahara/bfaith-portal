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
const REQUEST_TIMEOUT_MS = 30000;

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

  let res;
  try {
    res = await fetchImpl(OPENAI_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
      body: JSON.stringify({
        model,
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user', content: userPrompt },
        ],
        // GPT-5系は temperature 指定不可 (既定のみ)・max_tokens ではなく max_completion_tokens
        max_completion_tokens: 2000,
      }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
  } catch (err) {
    const timedOut = err?.name === 'TimeoutError' || err?.name === 'AbortError';
    throw new Error(timedOut ? 'AI書き換えがタイムアウトしました。もう一度お試しください' : `AI接続エラー: ${String(err?.message || err).slice(0, 120)}`);
  }

  const j = await res.json().catch(() => null);
  if (!res.ok) {
    // エラー本文からはOpenAIのmessageだけ拾う (キー誤り・モデル名誤り・残高切れが典型)
    const msg = j?.error?.message || `HTTP ${res.status}`;
    if (res.status === 401) throw new Error('AI書き換えの認証に失敗しました (OPENAI_API_KEY を確認してください)');
    if (res.status === 404) throw new Error(`AIモデルが見つかりません (OPENAI_REWRITE_MODEL='${model}' を確認してください)`);
    if (res.status === 429) throw new Error('AIが混み合っています (残高不足の可能性もあります)。少し待って再試行してください');
    throw new Error(`AI書き換えに失敗しました: ${String(msg).slice(0, 200)}`);
  }

  const out = String(j?.choices?.[0]?.message?.content || '').trim();
  if (!out) throw new Error('AIから空の応答が返りました。もう一度お試しください');
  return { text: out, model };
}
