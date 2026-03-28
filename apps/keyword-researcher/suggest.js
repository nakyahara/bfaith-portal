/**
 * Amazon サジェスト取得モジュール
 * completion.amazon.co.jp の公開APIを使用
 */

const SUGGEST_URL = 'https://completion.amazon.co.jp/api/2017/suggestions';
const MARKETPLACE_ID = 'A1VC38T7YXB528'; // Amazon.co.jp

// 五十音 + アルファベット（掛け合わせ用）
const HIRAGANA = [
  'あ','い','う','え','お','か','き','く','け','こ',
  'さ','し','す','せ','そ','た','ち','つ','て','と',
  'な','に','ぬ','ね','の','は','ひ','ふ','へ','ほ',
  'ま','み','む','め','も','や','ゆ','よ',
  'ら','り','る','れ','ろ','わ','を','ん',
];
const ALPHABET = 'abcdefghijklmnopqrstuvwxyz'.split('');

/**
 * 単一キーワードのサジェストを取得
 * @param {string} prefix - 検索プレフィックス
 * @returns {Promise<string[]>} サジェスト候補の配列
 */
async function fetchSuggestions(prefix) {
  const params = new URLSearchParams({
    mid: MARKETPLACE_ID,
    alias: 'aps',
    prefix,
  });

  try {
    const res = await fetch(`${SUGGEST_URL}?${params}`, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
      },
    });

    if (!res.ok) {
      throw new Error(`Amazon API error: ${res.status}`);
    }

    const data = await res.json();
    const suggestions = (data.suggestions || []).map(s => s.value);
    return suggestions;
  } catch (err) {
    console.error(`[Suggest] "${prefix}" 取得エラー:`, err.message);
    return [];
  }
}

/**
 * レート制限付きの遅延
 */
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * キーワードのサジェストを網羅的に取得
 * @param {string} seed - シードキーワード
 * @param {object} options
 * @param {boolean} options.hiragana - 五十音掛け合わせ（デフォルト: true）
 * @param {boolean} options.alphabet - アルファベット掛け合わせ（デフォルト: false）
 * @param {number} options.depth - 深掘り階層数（デフォルト: 1）
 * @param {number} options.delayMs - リクエスト間隔ms（デフォルト: 200）
 * @returns {Promise<object>} { seed, suggestions: [{keyword, source, depth}] }
 */
async function getSuggestions(seed, options = {}) {
  const {
    hiragana = true,
    alphabet = false,
    depth = 1,
    delayMs = 200,
  } = options;

  const allKeywords = new Map(); // keyword -> { source, depth }

  // 1. ベースサジェスト取得
  const baseSuggestions = await fetchSuggestions(seed);
  for (const kw of baseSuggestions) {
    allKeywords.set(kw, { source: 'base', depth: 0 });
  }

  // 2. 五十音掛け合わせ
  if (hiragana) {
    for (const char of HIRAGANA) {
      await delay(delayMs);
      const suggestions = await fetchSuggestions(`${seed} ${char}`);
      for (const kw of suggestions) {
        if (!allKeywords.has(kw)) {
          allKeywords.set(kw, { source: `hiragana:${char}`, depth: 0 });
        }
      }
    }
  }

  // 3. アルファベット掛け合わせ
  if (alphabet) {
    for (const char of ALPHABET) {
      await delay(delayMs);
      const suggestions = await fetchSuggestions(`${seed} ${char}`);
      for (const kw of suggestions) {
        if (!allKeywords.has(kw)) {
          allKeywords.set(kw, { source: `alphabet:${char}`, depth: 0 });
        }
      }
    }
  }

  // 4. 深掘り（depth >= 2 の場合、取得したサジェストをさらに展開）
  if (depth >= 2) {
    const level1Keywords = [...allKeywords.keys()];
    for (const kw of level1Keywords) {
      await delay(delayMs);
      const deeper = await fetchSuggestions(kw);
      for (const dkw of deeper) {
        if (!allKeywords.has(dkw)) {
          allKeywords.set(dkw, { source: `deep:${kw}`, depth: 1 });
        }
      }
    }
  }

  // 結果を配列に変換
  const suggestions = [];
  for (const [keyword, meta] of allKeywords) {
    if (keyword.toLowerCase() !== seed.toLowerCase()) {
      suggestions.push({ keyword, ...meta });
    }
  }

  // アルファベット順にソート
  suggestions.sort((a, b) => a.keyword.localeCompare(b.keyword, 'ja'));

  return { seed, total: suggestions.length, suggestions };
}

export { fetchSuggestions, getSuggestions, HIRAGANA, ALPHABET };
