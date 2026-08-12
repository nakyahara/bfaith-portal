/**
 * Notion「スタッフ用デイリー業務」との進捗連携 (fail-soft)。
 *
 * 現行運用: 出荷進捗は Notion DB (c87194a1-51f4-4108-a21a-5514c932c27b) のカンバンで管理し、
 * カード = 出荷No単位 (タイトル「出荷_18」・毎日作成)。ステータスを人が動かしている。
 * 本モジュールはスマホピッキングの操作に合わせてカードを自動で動かす:
 *   ピッキング開始 → 「ピッキング中」 / 完了 → 「ピッキング完了」 / 完了取り消し → 「ピッキング中」
 *
 * 方針:
 *   - 連携失敗でピッキング作業を止めない (呼び出し側は fire-and-forget + ログのみ)
 *   - トークンはツール別 (env PICKING_NOTION_TOKEN)。未設定なら何もしない (導入前でも動く)
 *   - プロパティ名は DB メタデータから実行時に発見してキャッシュ (title プロパティ /
 *     status・select 型の「ステータス」)。Notion 側の改名に設定変更なしで追従はしないが、
 *     見つからない場合は明確なログを吐く
 *   - カードは「タイトル = 出荷_XX かつ 今日作成」のみ対象。昨日のカードは動かさない
 *     (出荷Noは毎日再利用されるため)
 */

const NOTION_DB_ID = process.env.PICKING_NOTION_DB || 'c87194a151f44108a21a5514c932c27b';
const NOTION_VERSION = '2022-06-28';
const TIMEOUT_MS = 8000;

export const STATUS_PICKING = 'ピッキング中';
export const STATUS_PICKED = 'ピッキング完了';

async function notionFetch(path, { method = 'GET', body } = {}) {
  const token = process.env.PICKING_NOTION_TOKEN;
  if (!token) return null;   // 未設定 = 連携オフ
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(`https://api.notion.com/v1${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        'Notion-Version': NOTION_VERSION,
        'Content-Type': 'application/json',
      },
      body: body ? JSON.stringify(body) : undefined,
      signal: ctrl.signal,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(`Notion API ${method} ${path} → ${res.status}: ${text.slice(0, 200)}`);
    }
    return await res.json();
  } finally {
    clearTimeout(timer);
  }
}

// DBメタデータ (title プロパティ名・ステータスプロパティ名/型) は起動中キャッシュ
let _schemaCache = null;
async function getSchema() {
  if (_schemaCache) return _schemaCache;
  const db = await notionFetch(`/databases/${NOTION_DB_ID}`);
  if (!db) return null;
  let titleProp = null;
  let statusProp = null;
  for (const [name, def] of Object.entries(db.properties || {})) {
    if (def.type === 'title') titleProp = name;
    if ((def.type === 'status' || def.type === 'select') && name === 'ステータス') {
      statusProp = { name, type: def.type };
    }
  }
  // 「ステータス」が無ければ status 型の唯一のプロパティを採用 (改名への保険)
  if (!statusProp) {
    const statusTyped = Object.entries(db.properties || {}).filter(([, d]) => d.type === 'status');
    if (statusTyped.length === 1) statusProp = { name: statusTyped[0][0], type: 'status' };
  }
  if (!titleProp || !statusProp) {
    throw new Error(`Notion DBのプロパティが見つかりません (title=${titleProp}, status=${statusProp?.name})`);
  }
  _schemaCache = { titleProp, statusProp };
  return _schemaCache;
}

/** JST 'YYYY-MM-DD' → その日のUTC区間 [start, end) */
function jstDayRangeUtc(jstDate) {
  const start = new Date(`${jstDate}T00:00:00+09:00`);
  const end = new Date(start.getTime() + 24 * 3600 * 1000);
  return { start: start.toISOString(), end: end.toISOString() };
}

/**
 * 「タイトル = folderName・今日 (JST) 作成」のカードを1枚探す。
 * 見つからない / 複数でも最新1枚 (同名の今日カードが2枚ある運用は無いが、あれば最新を正とする)。
 */
async function findTodayCard(folderName, jstDate) {
  const schema = await getSchema();
  if (!schema) return null;
  const q = await notionFetch(`/databases/${NOTION_DB_ID}/query`, {
    method: 'POST',
    body: {
      filter: { property: schema.titleProp, title: { equals: folderName } },
      sorts: [{ timestamp: 'created_time', direction: 'descending' }],
      page_size: 5,
    },
  });
  if (!q) return null;
  const { start, end } = jstDayRangeUtc(jstDate);
  return (q.results || []).find((p) => p.created_time >= start && p.created_time < end) || null;
}

/**
 * カードのステータスを変更する。対象カードが無ければ何もしない (エラーにしない —
 * Notion側のカード作成が遅れている朝一などに作業を止めないため)。
 * @returns {'updated'|'no_card'|'disabled'}
 */
export async function syncPickingStatus(folderName, statusLabel, jstDate) {
  if (!process.env.PICKING_NOTION_TOKEN) return 'disabled';
  if (!folderName) return 'no_card';
  const card = await findTodayCard(folderName, jstDate);
  if (!card) {
    console.warn(`[picking-notion] 今日の「${folderName}」カードが見つかりません (${statusLabel} へ変更なし)`);
    return 'no_card';
  }
  const { statusProp } = await getSchema();
  await notionFetch(`/pages/${card.id}`, {
    method: 'PATCH',
    body: {
      properties: {
        [statusProp.name]: statusProp.type === 'status'
          ? { status: { name: statusLabel } }
          : { select: { name: statusLabel } },
      },
    },
  });
  console.log(`[picking-notion] ${folderName} → ${statusLabel}`);
  return 'updated';
}
