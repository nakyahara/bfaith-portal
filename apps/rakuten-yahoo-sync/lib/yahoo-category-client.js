/**
 * Phase E-17: Yahoo Shopping categorySearch API クライアント。
 *
 *   GET https://shopping.yahooapis.jp/ShoppingWebService/V1/json/categorySearch?appid=<ClientID>&category_id=<id>
 *     - appid (Client ID) のみで認証、 OAuth 不要 → Render 直で叩ける (公開カテゴリマスタ API)。
 *     - category_id=1 でルート + 第1階層、 各 category_id で Current + その直下 Children を返す。
 *
 *   レスポンス構造 (XML を JSON 化したもの、 値が配列/単一で揺れるため tolerant に拾う):
 *     ResultSet.Result.Categories.Current  — 指定カテゴリ自身 (Id, ParentId, Title{Short,Medium,Long}, Path)
 *     ResultSet.Result.Categories.Children — 直下の子カテゴリ群 (Category[] または単一)
 *
 *   path は Current.Path (祖先 + 自身の階層) の Title.Medium を ':' 連結して生成する。
 *   既存 category_default_path の表記 (例「生活雑貨・日用品:掃除用品」) に合わせ、 ルート (Depth=0「すべて」) は除外。
 */

const CATEGORY_SEARCH_URL = 'https://shopping.yahooapis.jp/ShoppingWebService/V1/json/categorySearch';
const DEFAULT_TIMEOUT_MS = 15_000;

export class YahooCategoryApiError extends Error {
  constructor(message, { status = null, body = null } = {}) {
    super(`[yahoo-category] ${message}`);
    this.name = 'YahooCategoryApiError';
    this.status = status;
    this.body = body;
  }
}

function getAppId() {
  const id = (process.env.YAHOO_APP_ID || '').trim();
  if (!id) throw new YahooCategoryApiError('YAHOO_APP_ID env not set');
  return id;
}

/** 値が配列でも単一オブジェクトでも配列に正規化 (XML→JSON の揺れ対策)。
 *  Codex E-17 R1 Medium: 繰り返しノードが numeric-key オブジェクト ({0:..,1:..}) で来るケースも
 *  Object.values で配列化する (Yahoo の JSON 化で起こりうる)。 */
function asArray(v) {
  if (v == null) return [];
  if (Array.isArray(v)) return v;
  if (typeof v === 'object') {
    const keys = Object.keys(v);
    if (keys.length > 0 && keys.every((k) => /^\d+$/.test(k))) return Object.values(v);
  }
  return [v];
}

/** Title が {Short,Medium,Long} でも文字列でも、 表示用 (medium 優先) を取り出す。 */
function pickTitle(title) {
  if (title == null) return { short: null, medium: null, long: null };
  if (typeof title === 'string') return { short: title, medium: title, long: title };
  const short = title.Short != null ? String(title.Short) : null;
  const medium = title.Medium != null ? String(title.Medium) : null;
  const long = title.Long != null ? String(title.Long) : null;
  return { short, medium, long };
}

/** 数値 ID を tolerant に取り出す (文字列で来る)。 */
function toIntOrNull(v) {
  if (v == null || v === '') return null;
  const n = parseInt(String(v), 10);
  return Number.isFinite(n) ? n : null;
}

/**
 * Current.Path (祖先 + 自身の階層) から ':' 連結のフルパスを生成。
 *   - Path.Category[] (または単一) を Depth 昇順で並べ、 ルート (Depth=0、 通常「すべて」) を除外。
 *   - 各階層は Title.Medium を採用 (既存 default_path 表記に合わせる)。
 *   - Path が取れない場合は null (呼び出し側で fallback)。
 */
export function buildPathFromCurrent(current) {
  if (!current || typeof current !== 'object') return null;
  const cats = asArray(current.Path?.Category ?? current.Path);
  if (cats.length === 0) return null;
  // Depth があれば昇順ソート、 無ければ与えられた順
  const sorted = cats
    .map((c) => ({
      depth: toIntOrNull(c?.Depth),
      title: pickTitle(c?.Title).medium,
      id: toIntOrNull(c?.Id),
    }))
    .filter((c) => c.title && c.title.trim() !== '');
  sorted.sort((a, b) => {
    if (a.depth == null || b.depth == null) return 0;
    return a.depth - b.depth;
  });
  // ルート (Depth=0 もしくは Id=1「すべて」) を除外
  const parts = sorted
    .filter((c) => !(c.depth === 0 || c.id === 1))
    .map((c) => c.title.trim());
  if (parts.length === 0) return null;
  return parts.join(':');
}

/**
 * categorySearch を 1 回叩いて、 指定カテゴリの Current + Children を正規化して返す。
 *
 * @param {number|string} categoryId
 * @param {object} [opts] { fetchImpl, timeoutMs }
 * @returns {Promise<{ current: object|null, children: object[], raw: object }>}
 *   current = { categoryId, parentId, titleShort, titleMedium, titleLong, path }
 *   children = [{ categoryId, parentId, titleShort, titleMedium, titleLong }]
 */
export async function fetchCategory(categoryId, opts = {}) {
  const appid = getAppId();
  const _fetch = opts.fetchImpl || fetch;
  const timeoutMs = opts.timeoutMs || DEFAULT_TIMEOUT_MS;
  const url = `${CATEGORY_SEARCH_URL}?appid=${encodeURIComponent(appid)}&category_id=${encodeURIComponent(String(categoryId))}`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let res;
  try {
    res = await _fetch(url, { signal: controller.signal });
  } catch (e) {
    throw new YahooCategoryApiError(`fetch failed: ${e.message}`);
  } finally {
    clearTimeout(timer);
  }
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new YahooCategoryApiError(`HTTP ${res.status}`, { status: res.status, body: body.slice(0, 500) });
  }
  let json;
  try {
    json = await res.json();
  } catch (e) {
    throw new YahooCategoryApiError(`invalid JSON: ${e.message}`);
  }
  return parseCategoryResponse(json);
}

/**
 * categorySearch の生 JSON を正規化。 テスト容易性のため export。
 */
export function parseCategoryResponse(json) {
  const categories = json?.ResultSet?.Result?.Categories
    ?? json?.ResultSet?.[0]?.Result?.Categories
    ?? null;
  if (!categories) {
    return { current: null, children: [], raw: json };
  }
  const cur = categories.Current;
  let current = null;
  if (cur) {
    const t = pickTitle(cur.Title);
    current = {
      categoryId: toIntOrNull(cur.Id),
      parentId: toIntOrNull(cur.ParentId),
      titleShort: t.short,
      titleMedium: t.medium,
      titleLong: t.long,
      path: buildPathFromCurrent(cur),
    };
  }
  const children = asArray(categories.Children?.Category ?? categories.Children)
    .map((c) => {
      const t = pickTitle(c?.Title);
      return {
        categoryId: toIntOrNull(c?.Id),
        parentId: toIntOrNull(c?.ParentId),
        titleShort: t.short,
        titleMedium: t.medium,
        titleLong: t.long,
      };
    })
    .filter((c) => c.categoryId != null);
  return { current, children, raw: json };
}
