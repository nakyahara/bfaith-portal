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

// 設定するステータス値 (env で上書き可)
function getStatusValue() { return process.env.FBA_PICKING_NOTION_STATUS_VALUE || '本日のやること'; }

// DB スキーマから タイトル/ステータス プロパティを検出。
// status: env FBA_PICKING_NOTION_STATUS_PROP 指定があればその名前、無ければ
//   type='status' を優先、無ければ type='select' の先頭を採用 ({name,type} or null)。
async function getSchema(dbId) {
  const db = await notionFetch(`/databases/${dbId}`, { method: 'GET' });
  const props = db.properties || {};
  let titleProp = null;
  for (const [name, def] of Object.entries(props)) {
    if (def && def.type === 'title') { titleProp = name; break; }
  }
  if (!titleProp) throw new Error('Notion DBにタイトルプロパティが見つかりません');

  let status = null;
  const wantName = (process.env.FBA_PICKING_NOTION_STATUS_PROP || '').trim();
  if (wantName && props[wantName] && (props[wantName].type === 'status' || props[wantName].type === 'select')) {
    status = { name: wantName, type: props[wantName].type };
  } else {
    for (const [name, def] of Object.entries(props)) {
      if (def && def.type === 'status') { status = { name, type: 'status' }; break; }
    }
    if (!status) {
      for (const [name, def] of Object.entries(props)) {
        if (def && def.type === 'select') { status = { name, type: 'select' }; break; }
      }
    }
  }
  return { titleProp, status };
}

/**
 * カードを作成。pdfUrl があれば external ファイルブロックで添付。
 * ステータス(セレクト/ステータス型)を「本日のやること」に設定。設定で失敗したらステータス無しで再作成
 * (カードは必ず作る)。
 * @returns {Promise<{ pageId: string, url: string, statusSet: boolean }>}
 */
export async function createPickingCard({ title, pdfUrl }) {
  if (!getToken()) throw new Error('FBA_PICKING_NOTION_TOKEN が未設定です');
  if (!title) throw new Error('カード名(納品予定日)が空です');
  const dbId = getNotionDbId();
  const { titleProp, status } = await getSchema(dbId);

  const children = [];
  if (pdfUrl) {
    children.push({
      object: 'block',
      type: 'file',
      file: { type: 'external', external: { url: pdfUrl }, caption: [] },
    });
  }

  const baseProps = { [titleProp]: { title: [{ text: { content: title } }] } };
  const statusProps = { ...baseProps };
  if (status) {
    statusProps[status.name] = status.type === 'status'
      ? { status: { name: getStatusValue() } }
      : { select: { name: getStatusValue() } };
  }
  const mkBody = (properties) => JSON.stringify({ parent: { database_id: dbId }, properties, children });

  if (status) {
    try {
      const page = await notionFetch('/pages', { method: 'POST', body: mkBody(statusProps) });
      return { pageId: page.id, url: page.url, statusSet: true };
    } catch (e) {
      // 400(validation_error)= ステータス指定が原因でNotion側はページ未作成と判断できる場合のみ、
      // ステータス無しで再作成する。タイムアウト/ネットワーク/5xx はページ作成済みの可能性があり
      // 重複作成を招くため再作成しない (Codex Medium)。
      if (e.status !== 400) throw e;
      console.error('[Notion] ステータス付き作成が400、ステータス無しで再作成:', e.message);
      const page = await notionFetch('/pages', { method: 'POST', body: mkBody(baseProps) });
      return { pageId: page.id, url: page.url, statusSet: false };
    }
  }
  const page = await notionFetch('/pages', { method: 'POST', body: mkBody(baseProps) });
  return { pageId: page.id, url: page.url, statusSet: false };
}
