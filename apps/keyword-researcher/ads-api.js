/**
 * Amazon Ads API クライアント
 * キーワード推奨・入札額取得
 */

const TOKEN_URL = 'https://api.amazon.co.jp/auth/o2/token';
const ADS_API_BASE = 'https://advertising-api-fe.amazon.com';

let cachedToken = null;
let tokenExpiresAt = 0;

/**
 * アクセストークンを取得（キャッシュ付き）
 */
async function getAccessToken() {
  if (cachedToken && Date.now() < tokenExpiresAt - 60000) {
    return cachedToken;
  }

  const res = await fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      refresh_token: process.env.ADS_REFRESH_TOKEN,
      client_id: process.env.ADS_CLIENT_ID,
      client_secret: process.env.ADS_CLIENT_SECRET,
    }).toString(),
  });

  const data = await res.json();
  if (data.error) {
    throw new Error(`Token error: ${data.error} - ${data.error_description || ''}`);
  }

  cachedToken = data.access_token;
  tokenExpiresAt = Date.now() + (data.expires_in * 1000);
  return cachedToken;
}

/**
 * Ads API にリクエスト
 */
async function adsRequest(method, path, body = null) {
  const token = await getAccessToken();
  const headers = {
    'Authorization': `Bearer ${token}`,
    'Amazon-Advertising-API-ClientId': process.env.ADS_CLIENT_ID,
    'Amazon-Advertising-API-Scope': process.env.ADS_PROFILE_ID,
    'Content-Type': 'application/json',
    'Accept': 'application/vnd.spkeywordrecommendation.v5+json',
  };

  const opts = { method, headers };
  if (body) opts.body = JSON.stringify(body);

  const res = await fetch(`${ADS_API_BASE}${path}`, opts);

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Ads API ${res.status}: ${text}`);
  }

  return res.json();
}

/**
 * ASINベースのキーワード推奨を取得
 * @param {string[]} asins - 対象ASIN（最大10個）
 * @param {number} maxRecommendations - 最大件数（デフォルト100）
 * @returns {Promise<object[]>} キーワード推奨リスト
 */
async function getKeywordRecommendations(asins, maxRecommendations = 100) {
  const body = {
    maxRecommendations,
    sortDimension: 'CONVERSIONS',
    locale: 'ja_JP',
    asins: asins.slice(0, 10),
  };

  const data = await adsRequest('POST', '/sp/targets/keywords/recommendations', body);
  return (data.keywordTargetList || []).map(item => ({
    keyword: item.keyword,
    matchType: item.matchType,
    bid: item.bid,
    userSelectedKeyword: item.userSelectedKeyword || false,
  }));
}

/**
 * キーワードベースのキーワード推奨を取得（関連キーワード拡張）
 * @param {string[]} keywords - 対象キーワード
 * @param {number} maxRecommendations - 最大件数
 * @returns {Promise<object[]>}
 */
async function getKeywordRecommendationsByKeyword(keywords, maxRecommendations = 100) {
  // keyword-based recommendations use targets endpoint
  const body = {
    maxRecommendations,
    sortDimension: 'CONVERSIONS',
    locale: 'ja_JP',
    targets: keywords.slice(0, 10).map(kw => ({
      keyword: kw,
      matchType: 'BROAD',
    })),
  };

  try {
    const data = await adsRequest('POST', '/sp/targets/keywords/recommendations', body);
    return (data.keywordTargetList || []).map(item => ({
      keyword: item.keyword,
      matchType: item.matchType,
      bid: item.bid,
    }));
  } catch (err) {
    console.error('[AdsAPI] keyword-based recommendations error:', err.message);
    return [];
  }
}

/**
 * テーマベースの入札推奨を取得（キーワードのランク・検索ボリューム推定）
 * @param {string[]} asins - 対象ASIN
 * @returns {Promise<object[]>}
 */
async function getThemeRecommendations(asins) {
  const body = {
    asins: asins.slice(0, 10),
    maxRecommendations: 200,
    locale: 'ja_JP',
  };

  try {
    const headers = {
      'Authorization': `Bearer ${await getAccessToken()}`,
      'Amazon-Advertising-API-ClientId': process.env.ADS_CLIENT_ID,
      'Amazon-Advertising-API-Scope': process.env.ADS_PROFILE_ID,
      'Content-Type': 'application/json',
      'Accept': 'application/vnd.spthemebasedbidrecommendation.v4+json',
    };

    const res = await fetch(`${ADS_API_BASE}/sp/targets/keywords/theme/recommendations`, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Theme API ${res.status}: ${text}`);
    }

    const data = await res.json();
    return (data.themeBasedBidRecommendationList || []).map(item => ({
      theme: item.theme,
      keywords: (item.keywordTargetList || []).map(kw => ({
        keyword: kw.keyword,
        matchType: kw.matchType,
        bid: kw.bid,
        rank: kw.rank,
      })),
    }));
  } catch (err) {
    console.error('[AdsAPI] theme recommendations error:', err.message);
    return [];
  }
}

/**
 * Ads API の接続テスト
 */
async function testConnection() {
  try {
    const token = await getAccessToken();
    const res = await fetch(`${ADS_API_BASE}/v2/profiles`, {
      headers: {
        'Authorization': `Bearer ${token}`,
        'Amazon-Advertising-API-ClientId': process.env.ADS_CLIENT_ID,
        'Content-Type': 'application/json',
      },
    });
    const profiles = await res.json();
    return { ok: true, profiles };
  } catch (err) {
    return { ok: false, error: err.message };
  }
}

/**
 * 設定チェック
 */
function isConfigured() {
  return !!(
    process.env.ADS_CLIENT_ID &&
    process.env.ADS_CLIENT_SECRET &&
    process.env.ADS_REFRESH_TOKEN &&
    process.env.ADS_PROFILE_ID
  );
}

export {
  getAccessToken,
  getKeywordRecommendations,
  getKeywordRecommendationsByKeyword,
  getThemeRecommendations,
  testConnection,
  isConfigured,
};
