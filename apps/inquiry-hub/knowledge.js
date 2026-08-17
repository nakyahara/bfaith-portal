/**
 * 社内ナレッジ検索 (2026-08-17) — AI下書きに「うちの会社の答え」を渡すための材料集め。
 *
 * 設計方針:
 * - AIは自社情報を知らない。何も渡さなければ事実を捏造する。
 *   そこで既存の Q&A (メールディーラー移行分) と返信テンプレートから
 *   「この問い合わせに関係しそうなもの」だけを選んで渡す。
 * - 選定は決定的なキーワードスコアリング (LLMに検索させない = 追加コスト・遅延なし・再現性あり)。
 * - 渡しすぎるとコストが増え精度も落ちるので上限を設ける。
 */
import { getDB } from './db.js';

export const MAX_QA = 6;
export const MAX_TEMPLATES = 3;
const MIN_SCORE = 3;           // これ未満は「関係なし」として渡さない
const SNIPPET_CHARS = 600;     // 1件あたりの本文引用の上限

/** 検索語の抽出: 日本語は2文字以上の連続語、英数字は3文字以上を拾う。
 * 助詞・定型句などノイズになる語は落とす (形態素解析は入れない = 依存を増やさない) */
const STOPWORDS = new Set([
  'ます', 'ください', 'お願い', 'よろしく', 'ありがとう', 'お世話', 'いたし', 'ござい',
  'こちら', 'そちら', 'それ', 'これ', 'ました', 'ません', 'です', 'でしょ', 'ますか',
  'したい', 'ですが', 'について', 'として', 'ような', 'という', 'なります', 'いただ',
  'the', 'and', 'for', 'you', 'your', 'this', 'that', 'with', 'have', 'from',
]);

export function extractKeywords(text, max = 30) {
  const s = String(text || '');
  const out = new Map();
  const add = (t, n = 1) => { if (!STOPWORDS.has(t) && t.length >= 2) out.set(t, (out.get(t) || 0) + n); };
  // 日本語 (漢字・カタカナ・ひらがな) の連続と、英数字の語をそれぞれ拾う
  const tokens = s.match(/[一-龠々]{2,}|[ァ-ヴー]{2,}|[ぁ-ん]{3,}|[A-Za-z0-9][A-Za-z0-9-]{2,}/g) || [];
  for (const raw of tokens) {
    const t = raw.toLowerCase();
    add(t, 2);
    // 形態素解析を入れない代わりに2文字n-gramも併用する
    // (「抽出方法」で書かれた質問が「抽出」としか書かれていないQ&Aに当たるようにする)
    if (/^[一-龠々ァ-ヴー]+$/.test(t) && t.length >= 3) {
      for (let i = 0; i + 2 <= t.length; i++) add(t.slice(i, i + 2));
    }
  }
  // 出現回数が多い順 (同数なら長い語を優先 = 具体的な語ほど効く)
  return [...out.entries()]
    .sort((a, b) => b[1] - a[1] || b[0].length - a[0].length)
    .slice(0, max)
    .map(([t]) => t);
}

/** 語の重み: 漢字・カタカナ (内容語) を厚く、ひらがな (多くは文法語) は薄く。
 * 「ありがとうございます」のような定型ひらがなだけで関連ありと判定されるのを防ぐ */
function weightOf(k) {
  if (/^[一-龠々ァ-ヴー]+$/.test(k)) return k.length >= 4 ? 3 : 2;
  if (/^[a-z0-9-]+$/.test(k)) return k.length >= 3 ? 2 : 1;
  return 1; // ひらがな主体
}

/** 1件のテキストに対するスコア: 含まれるキーワードの重み合計 */
function scoreText(haystack, keywords) {
  const h = String(haystack || '').toLowerCase();
  if (!h) return 0;
  let score = 0;
  for (const k of keywords) {
    if (h.includes(k)) score += weightOf(k);
  }
  return score;
}

const snip = (s, n = SNIPPET_CHARS) => {
  const t = String(s || '').replace(/\s+/g, ' ').trim();
  return t.length > n ? t.slice(0, n) + '…' : t;
};

/**
 * 問い合わせ内容に関係しそうな社内ナレッジを集める。
 * @param {object} p { subject, body, productName, productCode }
 * @returns {{ qa: Array, templates: Array, keywords: string[] }}
 */
export function findRelevantKnowledge({ subject = '', body = '', productName = '', productCode = '' } = {}) {
  const db = getDB();
  // 件名と商品名は「何についての問い合わせか」を強く表すので2回数えて重みを上げる
  const keywords = extractKeywords(`${subject} ${subject} ${productName} ${productName} ${productCode} ${body}`);
  if (!keywords.length) return { qa: [], templates: [], keywords: [] };

  const qaRows = db.prepare(`SELECT id, category, title, question, answer, notes
    FROM qa_entries WHERE is_active = 1 AND is_published = 1`).all();
  const qa = qaRows
    .map(r => ({ r, score: scoreText(`${r.title} ${r.question} ${r.category} ${r.answer}`, keywords) }))
    .filter(x => x.score >= MIN_SCORE)
    .sort((a, b) => b.score - a.score)
    .slice(0, MAX_QA)
    .map(x => ({
      id: x.r.id, category: x.r.category || '', title: x.r.title,
      question: snip(x.r.question, 300), answer: snip(x.r.answer),
      notes: snip(x.r.notes, 200), score: x.score,
    }));

  const tplRows = db.prepare(`SELECT id, category, template_name, subject, template_body, body_bottom, keywords
    FROM reply_templates WHERE is_active = 1`).all();
  const templates = tplRows
    .map(r => ({ r, score: scoreText(`${r.template_name} ${r.category} ${r.keywords} ${r.subject} ${r.template_body}`, keywords) }))
    .filter(x => x.score >= MIN_SCORE)
    .sort((a, b) => b.score - a.score)
    .slice(0, MAX_TEMPLATES)
    .map(x => ({
      id: x.r.id, category: x.r.category || '', name: x.r.template_name,
      body: snip(x.r.template_body), bodyBottom: snip(x.r.body_bottom, 300), score: x.score,
    }));

  return { qa, templates, keywords };
}

/** AIプロンプトに埋め込む文字列へ整形 (見つからなければ空文字) */
export function formatKnowledgeForPrompt({ qa, templates }) {
  const parts = [];
  if (qa.length) {
    parts.push('【社内Q&A (この会社の正しい回答。事実はここから引用する)】\n' + qa.map((q, i) =>
      `${i + 1}. ${q.category ? `[${q.category}] ` : ''}${q.title}\n   Q: ${q.question}\n   A: ${q.answer}${q.notes ? `\n   備考: ${q.notes}` : ''}`).join('\n'));
  }
  if (templates.length) {
    parts.push('【返信テンプレート (文体・言い回しの手本)】\n' + templates.map((t, i) =>
      `${i + 1}. ${t.category ? `[${t.category}] ` : ''}${t.name}\n${t.body}${t.bodyBottom ? `\n${t.bodyBottom}` : ''}`).join('\n---\n'));
  }
  return parts.join('\n\n');
}
