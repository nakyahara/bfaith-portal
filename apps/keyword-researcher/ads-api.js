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

/** env が揃っているか (未設定ならトークンAPIすら叩かない — fail-fast) */
function adsIsConfigured() {
  return !!(process.env.ADS_CLIENT_ID && process.env.ADS_CLIENT_SECRET
    && process.env.ADS_REFRESH_TOKEN && process.env.ADS_PROFILE_ID);
}

/** v3 list系 API 用 (versioned media type が必要。既存 adsRequest は推奨API固定のため別建て) */
async function adsRequestVersioned(method, path, body, mediaType) {
  const token = await getAccessToken();
  const res = await fetch(`${ADS_API_BASE}${path}`, {
    method,
    headers: {
      'Authorization': `Bearer ${token}`,
      'Amazon-Advertising-API-ClientId': process.env.ADS_CLIENT_ID,
      'Amazon-Advertising-API-Scope': process.env.ADS_PROFILE_ID,
      'Content-Type': mediaType,
      'Accept': mediaType,
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Ads API ${res.status}: ${text.slice(0, 300)}`);
  }
  return res.json();
}

// オートターゲティングのプレースホルダ (実測 2026-08-04: keywords/list に
// "(_targeting_auto_)" が混ざる)。マニュアルで人が入れたKWだけを扱うため除外する
const AUTO_PLACEHOLDER_RE = /^\(_.*_\)$/;

/**
 * pure: productAds と keywords を adGroupId で join して ASIN → マニュアルKW集合。
 * オートのプレースホルダKWは除外 (中原さん指示: マニュアルでなければ参照しない)
 */
function joinSpKeywordsByAsin(productAds, keywords) {
  const kwByAdGroup = new Map();
  for (const k of keywords || []) {
    const text = String(k?.keywordText || '').trim();
    if (!k?.adGroupId || !text || AUTO_PLACEHOLDER_RE.test(text)) continue;
    if (!kwByAdGroup.has(k.adGroupId)) kwByAdGroup.set(k.adGroupId, new Set());
    kwByAdGroup.get(k.adGroupId).add(text);
  }
  const byAsin = new Map();
  for (const ad of productAds || []) {
    const asin = String(ad?.asin || '').trim().toUpperCase();
    const kws = ad?.adGroupId ? kwByAdGroup.get(ad.adGroupId) : null;
    if (!asin || !kws || kws.size === 0) continue;
    if (!byAsin.has(asin)) byAsin.set(asin, new Set());
    for (const kw of kws) byAsin.get(asin).add(kw);
  }
  return byAsin;
}

let spKwCache = null; // { fetchedAt, byAsin }
const SP_KW_CACHE_TTL_MS = 30 * 60 * 1000;

/**
 * SP広告に「人がマニュアルで設定している」キーワードを ASIN 別に返す (ENABLEDのみ)。
 * アカウント全体を一括取得して30分キャッシュ (claim のたびに叩かない)。
 * env 未設定なら null (呼び出し側は fail-soft で続行する)。
 */
async function listSpManualKeywordsByAsin({ force = false } = {}) {
  if (!adsIsConfigured()) return null;
  if (!force && spKwCache && (Date.now() - spKwCache.fetchedAt) < SP_KW_CACHE_TTL_MS) {
    return spKwCache.byAsin;
  }
  const fetchAll = async (path, media, listKey) => {
    const out = [];
    let nextToken;
    for (let page = 0; page < 60; page++) {
      const body = { stateFilter: { include: ['ENABLED'] }, maxResults: 500, ...(nextToken ? { nextToken } : {}) };
      const data = await adsRequestVersioned('POST', path, body, media);
      out.push(...(data?.[listKey] || []));
      nextToken = data?.nextToken;
      if (!nextToken) break;
    }
    return out;
  };
  const [ads, kws] = await Promise.all([
    fetchAll('/sp/productAds/list', 'application/vnd.spProductAd.v3+json', 'productAds'),
    fetchAll('/sp/keywords/list', 'application/vnd.spKeyword.v3+json', 'keywords'),
  ]);
  const byAsin = joinSpKeywordsByAsin(ads, kws);
  spKwCache = { fetchedAt: Date.now(), byAsin };
  return byAsin;
}

export {
  getAccessToken,
  getKeywordRecommendations,
  adsIsConfigured,
  joinSpKeywordsByAsin,
  listSpManualKeywordsByAsin,
  getKeywordRecommendationsByKeyword,
  getThemeRecommendations,
  testConnection,
  isConfigured,
};
