/**
 * ChatGPT へ貼る定型文 (2026-08-26 現場要望)。画像制作カードの「初動判定を準備」「商品分析を準備」ボタンが使う。
 * 文面はここ 1 箇所で管理する (Ver 更新時に画面を触らない)。差し込む値はカードの登録値。
 * 両方とも「商品情報 (手入力)」が入っているときだけ使える (画面側で disabled + ここでも available=false)。
 */

const blank = (v) => v == null || String(v).trim() === '';

/** Amazon 商品 URL。登録が無ければ ASIN から組み立てる (Codex R1 見落とし指摘) */
export function amazonUrlOf(draft) {
  if (!blank(draft?.amazon_url)) return String(draft.amazon_url).trim();
  if (!blank(draft?.asin)) return `https://www.amazon.co.jp/dp/${String(draft.asin).trim()}`;
  return '';
}

export function buildInitialJudgePrompt(draft, ip) {
  return [
    '【入力】',
    ` Amazon商品URL： ${amazonUrlOf(draft) || '(未登録)'}`,
    '',
    '商品画像： ※画像はChatGPTへ手動で添付',
    '',
    `商品情報： ${String(ip?.product_info_text || '').trim()}`,
    '',
    '【実行】',
    ' 新商品初動判定 Ver1.0で分析してください。',
  ].join('\n');
}

/** 補足情報は画面で編集するので {{SUPPLEMENT}} を置換する */
export function buildProductAnalysisPrompt(draft, ip) {
  return [
    'LP制作システム V2.1',
    '',
    '① 商品分析',
    '',
    '商品名：',
    String(draft?.name || '').trim(),
    '',
    '商品説明：',
    String(ip?.product_info_text || '').trim(),
    '',
    '商品画像：',
    '※ChatGPTで手動添付',
    '',
    '補足情報：',
    '{{SUPPLEMENT}}',
  ].join('\n');
}

export function buildPromptTemplates(draft, ip) {
  const available = !blank(ip?.product_info_text);
  return {
    available,
    reason: available ? null : '「商品情報」を入力して保存すると使えます',
    initialJudge: available ? buildInitialJudgePrompt(draft, ip) : null,
    productAnalysis: available ? buildProductAnalysisPrompt(draft, ip) : null,
  };
}
