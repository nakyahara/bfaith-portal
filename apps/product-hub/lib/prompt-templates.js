/**
 * ChatGPT へ貼る定型文 (2026-08-26 現場要望)。画像制作カードの「初動判定を準備」「商品分析を準備」ボタンが使う。
 * 文面はここ 1 箇所で管理する (Ver 更新時に画面を触らない)。差し込む値はカードの登録値。
 *
 * 2026-09-10 スタッフ要望で文面を差し替え:
 *   - 貼ったらそのまま ChatGPT へ投げられる形にする (@GPT 名 + 参照仕様書 URL + 【入力】【実行】)
 *   - 商品画像は行だけ置いて空にする (ChatGPT へ直接貼り付けるので本文には入れない)
 *   - 商品情報 = 裏面情報 (パッケージ裏面の文字起こし) + 商品情報 の 2 つを連結して差し込む
 * 「商品情報」も「裏面情報」も空のときだけ使えない (画面側で disabled + ここでも available=false)。
 */

const blank = (v) => v == null || String(v).trim() === '';

/** 参照仕様書 (スタッフ管理のスプレッドシート)。Ver 更新でここだけ差し替える */
const SPEC_URL_INITIAL_JUDGE = 'https://docs.google.com/spreadsheets/d/1u2Qg2BTc34bBCqbaaA75FUNG5SXOrvupZqseqZQ2IB8/edit';
const SPEC_URL_PRODUCT_ANALYSIS = 'https://docs.google.com/spreadsheets/d/1CGQXKtz4E4Il-jkzYO3QL9oi2PAdS51-s4rStulHdYc/edit';

/** Amazon 商品 URL。登録が無ければ ASIN から組み立てる (Codex R1 見落とし指摘) */
export function amazonUrlOf(draft) {
  if (!blank(draft?.amazon_url)) return String(draft.amazon_url).trim();
  if (!blank(draft?.asin)) return `https://www.amazon.co.jp/dp/${String(draft.asin).trim()}`;
  return '';
}

/**
 * 定型文に差し込む「商品情報」。裏面情報 (任意) と 商品情報 を見出し付きで連結する。
 * 見出しを付けるのは、ChatGPT 側でどちらの出所か分かるようにするため (片方だけでも付ける)。
 * 見出しの記号が【】でないのは、裏面情報が【原材料】【内容量】のような表記をそのまま含むため
 * (同じ記号だと、どこまでが裏面情報か読み取れなくなる)。
 */
export function composeProductInfo(ip) {
  const parts = [];
  if (!blank(ip?.back_info_text)) parts.push(`■裏面情報 (パッケージ裏面の表記)\n${String(ip.back_info_text).trim()}`);
  if (!blank(ip?.product_info_text)) parts.push(`■商品情報\n${String(ip.product_info_text).trim()}`);
  return parts.join('\n\n');
}

export function buildInitialJudgePrompt(draft, ip) {
  return [
    '@新商品初動判定 Ver1.0',
    '',
    '【参照仕様書】',
    SPEC_URL_INITIAL_JUDGE,
    '',
    '【入力】',
    `Amazon商品URL：${amazonUrlOf(draft) || '(未登録)'}`,
    '商品画像：',
    '商品情報：',
    composeProductInfo(ip),
    '',
    '【実行】',
    '上記の商品情報と「新商品初動判定 Ver1.0」の仕様を使用して分析してください。',
    '不足情報がある場合も質問だけで止めず、確認できる情報の範囲で判定結果まで出力してください。',
  ].join('\n');
}

export function buildProductAnalysisPrompt(draft, ip) {
  return [
    '@LP制作システム V2.1',
    '',
    '【参照仕様書】',
    SPEC_URL_PRODUCT_ANALYSIS,
    '',
    '【入力】',
    '',
    `商品名：${String(draft?.name || '').trim()}`,
    '',
    '商品情報：',
    composeProductInfo(ip),
    '',
    '商品画像：',
    '',
    '',
    '【実行】',
    'LP制作システム V2.1の仕様に従い、内部で①商品分析〜⑥制作指示書まで検討してください。',
    'ただし、それらの途中結果は表示せず、最終回答は必ず⑦AI画像生成プロンプトのみを出力してください。',
    '',
    '出力形式は必ず以下を満たしてください。',
    '- 冒頭は「# LP制作システム V2.1」→「## ⑦ AI画像生成プロンプト」→「### AI画像生成プロンプト 出力テンプレート V2.2」',
    '- その後に「# 共通生成条件」「# 共通使用カラー」「# 商品再現ルール」を出力',
    '- 各画像は必ず「# 1枚目｜FV」のような見出し形式で出力',
    '- 各画像内は指定の見出しを固定で出力',
    '- 最後に「# 共通NG事項」「# 共通生成後チェック」を出力',
    '- 「①〜⑥」「最終まとめ」「補足説明」「必要なら次に〜」は出力しない',
    '- 不足情報があっても止まらず、「未確認」「要確認」と明記して最後まで出力する',
  ].join('\n');
}

export function buildPromptTemplates(draft, ip) {
  // 裏面情報は任意なので、どちらか一方でも入っていれば作れる (2026-09-10 スタッフ要望)
  const available = !blank(ip?.product_info_text) || !blank(ip?.back_info_text);
  return {
    available,
    reason: available ? null : '「商品情報」か「裏面情報」を入力して保存すると使えます',
    initialJudge: available ? buildInitialJudgePrompt(draft, ip) : null,
    productAnalysis: available ? buildProductAnalysisPrompt(draft, ip) : null,
  };
}
