/**
 * Notion 連携 (FBA納品ピッキング準備)
 * 処理実行時に、納品予定日のカードを Notion データベースに作成し、公開PDFを添付する。
 *
 * トークンは本アプリ専用に新規発行した Integration を使う想定 (env: FBA_PICKING_NOTION_TOKEN)。
 * その Integration を対象データベースに「接続(共有)」しておく必要がある。
 * 添付は公開PDF URL を external ファイルブロックとしてカード本文に置く方式 (アップロード不要)。
 */
const NOTION_VERSION = '2022-06-28';
const API = 'https://api.notion.com/v1';
const DEFAULT_DB = 'c87194a151f44108a21a5514c932c27b';

function getToken() { return process.env.FBA_PICKING_NOTION_TOKEN || ''; }
export function getNotionDbId() { return process.env.FBA_PICKING_NOTION_DB || DEFAULT_DB; }
export function notionConfigured() { return !!getToken(); }

async function notionFetch(path, opts = {}) {
  const res = await fetch(`${API}${path}`, {
    ...opts,
    headers: {
      Authorization: `Bearer ${getToken()}`,
      'Notion-Version': NOTION_VERSION,
      'Content-Type': 'application/json',
      ...(opts.headers || {}),
    },
    signal: AbortSignal.timeout(20000),
  });
  const text = await res.text().catch(() => '');
  let json = {};
  try { json = text ? JSON.parse(text) : {}; } catch { json = {}; }
  if (!res.ok) {
    const err = new Error(`Notion API ${path}: ${json?.message || `HTTP ${res.status}`}`);
    err.status = res.status;
    throw err;
  }
  return json;
}

// DB のタイトルプロパティ名を取得 (プロパティ名はDBによって異なるため動的に検出)
async function getTitlePropName(dbId) {
  const db = await notionFetch(`/databases/${dbId}`, { method: 'GET' });
  for (const [name, def] of Object.entries(db.properties || {})) {
    if (def && def.type === 'title') return name;
  }
  throw new Error('Notion DBにタイトルプロパティが見つかりません');
}

/**
 * カードを作成。pdfUrl があれば external ファイルブロックで添付。
 * @returns {Promise<{ pageId: string, url: string }>}
 */
export async function createPickingCard({ title, pdfUrl }) {
  if (!getToken()) throw new Error('FBA_PICKING_NOTION_TOKEN が未設定です');
  if (!title) throw new Error('カード名(納品予定日)が空です');
  const dbId = getNotionDbId();
  const titleProp = await getTitlePropName(dbId);

  const children = [];
  if (pdfUrl) {
    children.push({
      object: 'block',
      type: 'file',
      file: { type: 'external', external: { url: pdfUrl }, caption: [] },
    });
  }
  const body = {
    parent: { database_id: dbId },
    properties: { [titleProp]: { title: [{ text: { content: title } }] } },
    children,
  };
  const page = await notionFetch('/pages', { method: 'POST', body: JSON.stringify(body) });
  return { pageId: page.id, url: page.url };
}
