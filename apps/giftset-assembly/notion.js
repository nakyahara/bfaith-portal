/**
 * ギフトセット作業カードを Notion に作成 (Render から直接)
 *
 * 環境変数:
 *   NOTION_TOKEN          … Notion インテグレーションのシークレット
 *   GIFTSET_NOTION_DB_ID  … 「ギフトセット作業」DB の ID
 * どちらか未設定なら NOTION_NOT_CONFIGURED を投げる (fail-closed)。
 * ピッキングデータ生成は Notion 未設定でも動くよう、呼び出し側で分離している。
 */
const API_BASE = 'https://api.notion.com/v1';
const NOTION_VERSION = '2022-06-28';

function getConfig() {
  const token = process.env.NOTION_TOKEN;
  const dbId = process.env.GIFTSET_NOTION_DB_ID;
  if (!token || !dbId) {
    const e = new Error('Notion 連携が未設定です (NOTION_TOKEN / GIFTSET_NOTION_DB_ID)');
    e.code = 'NOTION_NOT_CONFIGURED';
    throw e;
  }
  return { token, dbId };
}

export function isNotionConfigured() {
  return !!(process.env.NOTION_TOKEN && process.env.GIFTSET_NOTION_DB_ID);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const backoff = (a) => 500 * 2 ** (a - 1) + Math.floor(Math.random() * 200); // 指数バックオフ + jitter

async function notionCreatePage(body, token) {
  const MAX = 3; // 429 / 5xx / 瞬断は指数バックオフで再試行
  let lastErr;
  for (let attempt = 1; attempt <= MAX; attempt++) {
    let res;
    try {
      res = await fetch(`${API_BASE}/pages`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Notion-Version': NOTION_VERSION,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
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

/**
 * 作業カードを1枚作成。
 * @param {object} args
 * @param {object} args.set        getGiftset() の戻り値 (giftset_name, photo_url, video_url, unit_price, components)
 * @param {number} args.qty        個数
 * @param {number|null} args.people 人数
 * @param {string|null} args.dueDate 納期 'YYYY-MM-DD'
 * @param {string} args.requestDate 依頼日 'YYYY-MM-DD'
 * @param {string|null} args.note  メモ
 * @returns {{ url: string, id: string }}
 */
export async function createWorkCard({ set, qty, people, dueDate, requestDate, note }) {
  const { token, dbId } = getConfig();

  const props = {
    '名前': { title: [{ text: { content: `${set.giftset_name}（${qty}個）` } }] },
    'ステータス': { select: { name: '未着手' } },
    '個数': { number: qty },
    '依頼日': { date: { start: requestDate } },
    'ギフトコード': { rich_text: [{ text: { content: set.giftset_code } }] },
  };
  if (Number.isInteger(people) && people > 0) props['人数'] = { number: people };
  if (dueDate) props['納期'] = { date: { start: dueDate } };
  if (set.unit_price != null) props['単価'] = { number: set.unit_price };
  if (set.video_url) props['動画リンク'] = { url: set.video_url };
  if (set.photo_url) {
    props['写真'] = { files: [{ type: 'external', name: '完成見本', external: { url: set.photo_url } }] };
  }
  if (note) props['メモ'] = { rich_text: [{ text: { content: String(note).slice(0, 1900) } }] };

  // ページ本文: 構成品リスト (1セットあたり / 合計)
  const children = [
    { object: 'block', type: 'heading_3', heading_3: { rich_text: [{ text: { content: '構成品（1セットあたり / 今回合計）' } }] } },
    ...set.components.map((c) => ({
      object: 'block', type: 'bulleted_list_item',
      bulleted_list_item: {
        rich_text: [{
          text: { content: `${c.商品名 || c.商品コード}（${c.商品コード}）× ${c.数量}　→ 合計 ${c.数量 * qty}` },
        }],
      },
    })),
  ];

  const data = await notionCreatePage({
    parent: { database_id: dbId },
    properties: props,
    children,
  }, token);

  return { url: data.url, id: data.id };
}
