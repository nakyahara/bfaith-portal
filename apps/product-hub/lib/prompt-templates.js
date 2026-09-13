/**
 * ChatGPT へ貼る定型文 (2026-08-26 現場要望)。画像制作カードの「初動判定を準備」「商品分析を準備」ボタンが使う。
 * 文面はここ 1 箇所で管理する (Ver 更新時に画面を触らない)。差し込む値はカードの登録値。
 *
 * 2026-09-10 スタッフ要望で文面を差し替え:
 *   - 貼ったらそのまま ChatGPT へ投げられる形にする (@GPT 名 + 参照仕様書 URL + 【入力】【実行】)
 *   - 商品画像は行だけ置いて空にする (ChatGPT へ直接貼り付けるので本文には入れない)
 *   - 商品情報 = 裏面情報 (パッケージ裏面の文字起こし) + 商品情報 の 2 つを連結して差し込む
 * 「商品情報」も「裏面情報」も空のときだけ使えない (画面側で disabled + ここでも available=false)。
 *
 * 2026-09-13 スタッフ要望で「初動判定」の文面を差し替え:
 *   - GPT 名から Ver を外す (「最新仕様を使用」に変わった)。参照仕様書は gid 付きの URL
 *   - 商品情報にカラバリを足す (NE のバリエーション構成 + 楽天の選択肢の値)
 *   - LP制作管理シート URL の行を置く (シートは人が作るので、貼り付け位置の案内文だけ)
 *   - 【実行】を「LP 構成から素材を逆算し、撮影の要否を決める」指示に
 */

const blank = (v) => v == null || String(v).trim() === '';

/** 参照仕様書 (スタッフ管理のスプレッドシート)。Ver 更新でここだけ差し替える */
const SPEC_URL_INITIAL_JUDGE = 'https://docs.google.com/spreadsheets/d/1u2Qg2BTc34bBCqbaaA75FUNG5SXOrvupZqseqZQ2IB8/edit?gid=11001#gid=11001';
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

/**
 * 定型文に差し込む「カラバリ」(2026-09-13)。NE を正とする (基本情報タブの判定と同じ)。
 * 各 SKU は楽天の選択肢の値 (「レッド」等) を優先し、未入力なら NE の商品名で出す。
 * 何も書かないと ChatGPT が「カラバリはありますか」と聞き返すので、単品でも「なし」と書く。
 *
 * @param {object} v
 * @param {{kind: string, members: object[]}|null} v.variation  resolveVariationGroup の戻り値
 * @param {{value: boolean}|null} v.hasVariation                 effectiveHasVariation の戻り値
 * @param {string|null} [v.selectorName]  楽天の項目選択肢の見出し (「カラー」「種類」)
 * @param {Record<string,string>} [v.selectorValues]  SKU別の選択肢の値。キーは LOWER(TRIM(商品コード))
 */
export function composeColorVariations({ variation, hasVariation, selectorName = null, selectorValues = {} } = {}) {
  const members = variation?.kind === 'variation' ? (variation.members || []) : [];
  if (members.length > 1) {
    const axis = blank(selectorName) ? '' : `${String(selectorName).trim()}・`;
    const lines = members.map((m) => {
      const value = selectorValues?.[String(m.商品コード || '').trim().toLowerCase()];
      return `・${!blank(value) ? String(value).trim() : (String(m.商品名 || '').trim() || String(m.商品コード || '').trim())}`;
    });
    return [`■カラバリ (${axis}全${members.length}種)`, ...lines].join('\n');
  }
  // NE 未登録の新商品は内訳が取れない。手入力の「バリエーションあり」だけ伝える
  if (hasVariation?.value) return '■カラバリ\nあり (NE 未登録のため内訳は未確認)';
  return '■カラバリ\nなし (単品)';
}

export function buildInitialJudgePrompt(draft, ip, colorVariations = '') {
  const productInfo = [composeProductInfo(ip), colorVariations].filter((t) => !blank(t)).join('\n\n');
  return [
    '@新商品初動判定',
    '',
    '【参照仕様書】',
    SPEC_URL_INITIAL_JUDGE,
    '',
    '【入力】',
    '',
    `Amazon商品URL：${amazonUrlOf(draft) || '(未登録)'}`,
    '',
    '商品画像：',
    '',
    '商品情報：',
    productInfo,
    '',
    'LP制作管理シートURL：',
    '（ここに作成したLP制作管理シートのURLを貼り付け）',
    '',
    '【実行】',
    '上記の商品情報・商品画像・LP制作管理シートと',
    '「新商品初動判定」の最新仕様を使用して分析してください。',
    '',
    'LP制作管理シート内のLP構成・AI生成用プロンプトを確認し、',
    '各画像に必要な素材を逆算してください。',
    '',
    '既存素材で足りるもの／図解・AIで作れるもの／追加撮影が必要なものを判定し、',
    'LPで実際に使える撮影素材を決めてください。',
    '',
    '不足情報がある場合も質問だけで止めず、',
    '確認できる範囲で判定結果まで出力してください。',
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

/**
 * @param {object} [variations] composeColorVariations の引数。初動判定だけが使う
 *   (商品分析の文面はカラバリを求めていない)
 */
export function buildPromptTemplates(draft, ip, variations = {}) {
  // 裏面情報は任意なので、どちらか一方でも入っていれば作れる (2026-09-10 スタッフ要望)
  const available = !blank(ip?.product_info_text) || !blank(ip?.back_info_text);
  return {
    available,
    reason: available ? null : '「商品情報」か「裏面情報」を入力して保存すると使えます',
    initialJudge: available ? buildInitialJudgePrompt(draft, ip, composeColorVariations(variations)) : null,
    productAnalysis: available ? buildProductAnalysisPrompt(draft, ip) : null,
  };
}
