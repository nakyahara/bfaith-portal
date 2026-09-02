/**
 * 入荷受付チェック — Notion「在庫化作業管理」DB クライアント
 *
 * いろは行きに確定した明細を Notion の作業カードにする経路の低レベル API。
 * 業務ロジック (どの行を送るか・取消の収束) は notion-sync.js が持つ。ここは HTTP だけ。
 *
 * 環境変数:
 *   NOTION_TOKEN                 … 既存インテグレーションのシークレット (giftset 等と共用。中原さん 2026-09-02)
 *   INBOUND_CHECK_NOTION_DB_ID   … 「在庫化作業管理」DB の ID
 * どちらか未設定なら NOTION_NOT_CONFIGURED (fail-closed)。
 *
 * ⭐カードには必ず destination_id (number) を入れる。
 *   「ページ作成は成功したが応答を受け取る前に落ちて page_id を保存できなかった」とき、
 *   このプロパティで既存カードを探して回収する (二重カードの防止。Codex設計相談R1 #3)。
 */

const API_BASE = 'https://api.notion.com/v1';
const NOTION_VERSION = '2022-06-28';

export function isNotionConfigured() {
  return !!(process.env.NOTION_TOKEN && process.env.INBOUND_CHECK_NOTION_DB_ID);
}

function getConfig() {
  const token = process.env.NOTION_TOKEN;
  const dbId = process.env.INBOUND_CHECK_NOTION_DB_ID;
  if (!token || !dbId) {
    const e = new Error('Notion 連携が未設定です (NOTION_TOKEN / INBOUND_CHECK_NOTION_DB_ID)');
    e.code = 'NOTION_NOT_CONFIGURED';
    throw e;
  }
  return { token, dbId };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const backoff = (a) => 500 * 2 ** (a - 1) + Math.floor(Math.random() * 200); // 指数バックオフ + jitter

/**
 * Notion API を1回叩く (429/5xx/瞬断は3回まで指数バックオフ)。
 * 4xx (認証・スキーマ不整合など) は再試行しても直らないので即 throw する。
 * throw する Error には code='NOTION_API_ERROR' と status (あれば) が付く。
 */
export async function notionRequest(path, method, body) {
  const { token } = getConfig();
  const MAX = 3;
  let lastErr;
  for (let attempt = 1; attempt <= MAX; attempt++) {
    let res;
    try {
      res = await fetch(`${API_BASE}${path}`, {
        method,
        headers: {
          'Authorization': `Bearer ${token}`,
          'Notion-Version': NOTION_VERSION,
          'Content-Type': 'application/json',
        },
        body: body != null ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(15000),
      });
    } catch (e) {
      lastErr = e;
      if (attempt < MAX) { await sleep(backoff(attempt)); continue; }
      const err = new Error(`Notion 接続エラー: ${e.message}`);
      err.code = 'NOTION_API_ERROR';
      throw err;
    }
    if (res.ok) {
      const data = await res.json().catch(() => ({}));
      if (data.object === 'error') {
        const err = new Error(`Notion API エラー: ${data.message || res.status}`);
        err.code = 'NOTION_API_ERROR'; err.status = res.status; throw err;
      }
      return data;
    }
    if ((res.status === 429 || res.status >= 500) && attempt < MAX) {
      const ra = Number(res.headers.get('retry-after'));
      await sleep(ra > 0 ? ra * 1000 : backoff(attempt));
      continue;
    }
    const data = await res.json().catch(() => ({}));
    const err = new Error(`Notion API エラー: ${data.message || res.status}`);
    err.code = 'NOTION_API_ERROR'; err.status = res.status; throw err;
  }
  const err = new Error(`Notion API エラー: ${lastErr?.message || 'unknown'}`);
  err.code = 'NOTION_API_ERROR'; throw err;
}

// ─── DB スキーマ (プロパティ) ───

// プロセス内キャッシュ。プロパティ名の集合はほぼ変わらないので毎 sweep 取り直さない
let schemaCache = null; // { at: ms, props: Set<string> }
const SCHEMA_TTL_MS = 10 * 60 * 1000;

/**
 * カード DB のプロパティ名一覧を返す。無ければ足すもの:
 *   destination_id (number)  … 二重カード防止の回収キー
 *   有効期限 (rich_text)     … YYYY-MM-DD / YYYY-MM の両方が入るので date 型にしない
 * 既存プロパティ (名前・ステータス・商品コード…) は一切触らない。
 * 戻り値の Set は「実在するプロパティ名」— カード作成側はこれに無い項目を送らない
 * (プロパティが改名されていても validation エラーで全滅しないため)。
 */
export async function ensureCardSchema({ force = false } = {}) {
  const { dbId } = getConfig();
  if (!force && schemaCache && Date.now() - schemaCache.at < SCHEMA_TTL_MS) return schemaCache.props;
  const db = await notionRequest(`/databases/${dbId}`, 'GET');
  const props = new Set(Object.keys(db.properties || {}));
  const add = {};
  if (!props.has('destination_id')) add['destination_id'] = { number: {} };
  if (!props.has('有効期限')) add['有効期限'] = { rich_text: {} };
  if (Object.keys(add).length > 0) {
    await notionRequest(`/databases/${dbId}`, 'PATCH', { properties: add });
    for (const k of Object.keys(add)) props.add(k);
  }
  schemaCache = { at: Date.now(), props };
  return props;
}

/** テストからキャッシュを消すため */
export function _clearSchemaCache() { schemaCache = null; }

// ─── ページ操作 ───

/** destination_id でカードを検索 (回収用)。見つからなければ null */
export async function findCardByDestinationId(destinationId) {
  const { dbId } = getConfig();
  const r = await notionRequest(`/databases/${dbId}/query`, 'POST', {
    filter: { property: 'destination_id', number: { equals: Number(destinationId) } },
    page_size: 1,
  });
  return r.results?.[0] || null;
}

/** カードを1枚作成 → { id, url } */
export async function createCard(properties) {
  const { dbId } = getConfig();
  const data = await notionRequest('/pages', 'POST', {
    parent: { database_id: dbId },
    properties,
  });
  return { id: data.id, url: data.url };
}

/** ページの現在状態 (取消反映の前に「人が動かしていたか」を見る) */
export async function getCardState(pageId) {
  const data = await notionRequest(`/pages/${pageId}`, 'GET');
  const status = data.properties?.['ステータス']?.select?.name || null;
  return { archived: !!data.archived, status };
}

/**
 * カードのステータスを変える (取消など)。
 * select 型は存在しない選択肢名を送ると Notion 側が自動で選択肢を作るので、
 * 「取消」を事前に手で足していなくても失敗しない。
 */
export async function setCardStatus(pageId, statusName) {
  await notionRequest(`/pages/${pageId}`, 'PATCH', {
    properties: { 'ステータス': { select: { name: statusName } } },
  });
}
