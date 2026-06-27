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

  return { titleProp, status: pickStatusProp(props), props };
}

// 指定名の URL(リンク)型プロパティが存在すれば名前を返す
function urlPropName(props, name) {
  return props[name] && props[name].type === 'url' ? name : null;
}

// カード本文(メモ)に置く「送り状発行手順」リンクの段落ブロック
function invoiceGuideBlock() {
  const url = process.env.FBA_PICKING_INVOICE_GUIDE_URL
    || 'https://app.notion.com/p/FBA-38cfc7bb5f3080ffbafdded6cee4727a';
  const label = process.env.FBA_PICKING_INVOICE_GUIDE_LABEL || '送り状発行手順';
  return {
    object: 'block',
    type: 'paragraph',
    paragraph: { rich_text: [{ type: 'text', text: { content: label, link: { url } } }] },
  };
}

// ステータス対象プロパティの決定 (優先順):
//  1. env FBA_PICKING_NOTION_STATUS_PROP で明示指定
//  2. 名前が「ステータス」/「Status」のプロパティ (DBに別名のselectがあるので名前で狙い撃ち)
//  3. status型の先頭
//  4. select型の先頭 (最終手段。複数selectがあると誤爆しうるので最後)
// @returns {{name, type}|null}
export function pickStatusProp(props = {}) {
  const isSel = (def) => def && (def.type === 'status' || def.type === 'select');
  const wantName = (process.env.FBA_PICKING_NOTION_STATUS_PROP || '').trim();
  if (wantName && isSel(props[wantName])) return { name: wantName, type: props[wantName].type };
  for (const cand of ['ステータス', 'Status', 'status', 'ステータス ']) {
    if (isSel(props[cand])) return { name: cand, type: props[cand].type };
  }
  for (const [name, def] of Object.entries(props)) {
    if (def && def.type === 'status') return { name, type: 'status' };
  }
  for (const [name, def] of Object.entries(props)) {
    if (def && def.type === 'select') return { name, type: 'select' };
  }
  return null;
}

/**
 * カードを作成。pdfUrl があれば external ファイルブロックで添付。
 * ステータス(セレクト/ステータス型)を「本日のやること」に設定。設定で失敗したらステータス無しで再作成
 * (カードは必ず作る)。
 * @returns {Promise<{ pageId: string, url: string, statusSet: boolean }>}
 */
export async function createPickingCard({ title, pdfUrl, plan1Url, plan2Url }) {
  if (!getToken()) throw new Error('FBA_PICKING_NOTION_TOKEN が未設定です');
  if (!title) throw new Error('カード名(納品予定日)が空です');
  const dbId = getNotionDbId();
  const { titleProp, status, props } = await getSchema(dbId);

  const children = [];
  if (pdfUrl) {
    children.push({
      object: 'block',
      type: 'file',
      file: { type: 'external', external: { url: pdfUrl }, caption: [] },
    });
  }
  // 本文(メモ)に「送り状発行手順」リンクを残す (コメントではなくページ本文)
  children.push(invoiceGuideBlock());

  // タイトル + URL_1/URL_2 (リンク型プロパティが存在し値がある場合のみ)
  const commonProps = { [titleProp]: { title: [{ text: { content: title } }] } };
  const p1 = urlPropName(props, process.env.FBA_PICKING_NOTION_URL1_PROP || 'URL_1');
  const p2 = urlPropName(props, process.env.FBA_PICKING_NOTION_URL2_PROP || 'URL_2');
  if (p1 && plan1Url) commonProps[p1] = { url: plan1Url };
  if (p2 && plan2Url) commonProps[p2] = { url: plan2Url };

  const statusProps = { ...commonProps };
  if (status) {
    statusProps[status.name] = status.type === 'status'
      ? { status: { name: getStatusValue() } }
      : { select: { name: getStatusValue() } };
  }
  const mkBody = (properties) => JSON.stringify({ parent: { database_id: dbId }, properties, children });

  // ページ作成 (ステータス付き → 400ならステータス無しで再作成。重複防止のため400限定)
  let page, statusSet = false;
  if (status) {
    try {
      page = await notionFetch('/pages', { method: 'POST', body: mkBody(statusProps) });
      statusSet = true;
    } catch (e) {
      if (e.status !== 400) throw e;
      console.error('[Notion] ステータス付き作成が400、ステータス無しで再作成:', e.message);
      page = await notionFetch('/pages', { method: 'POST', body: mkBody(commonProps) });
    }
  } else {
    page = await notionFetch('/pages', { method: 'POST', body: mkBody(commonProps) });
  }

  return { pageId: page.id, url: page.url, statusSet };
}
