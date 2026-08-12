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
    console.warn(`[picking-notion] ${jstDate} の「${folderName}」カードが見つかりません (${statusLabel} へ変更なし)`);
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

/**
 * バッチの「現在の状態」をNotionへ反映する (状態はこの関数の実行時点で読み直す)。
 *
 * ⭐イベント時点のラベルを fire-and-forget で送ると、並行実行で PATCH の到着順が逆転し
 * 「completed の後に遅れた started が届いて完了カードが戻る」事故が起きる (Codex PR3 high)。
 * バッチごとに Promise チェーンで直列化し、送信直前に最新状態を読むことで
 * 「最後に実行されたものが最新状態を送る」を保証する (途中の遷移はスキップされてよい)。
 *
 * @param getBatchState () => ({folderName, workDate, label|null}) — 実行時点の状態を返す
 */
const _batchQueues = new Map();   // batchId → Promise (直列化チェーン)
export function enqueueBatchSync(batchId, getBatchState) {
  if (!process.env.PICKING_NOTION_TOKEN) return;
  const prev = _batchQueues.get(batchId) || Promise.resolve();
  const next = prev
    .then(async () => {
      const state = getBatchState();
      if (!state || !state.label) return;
      try {
        await syncPickingStatus(state.folderName, state.label, state.workDate);
      } catch (e) {
        // 一時障害向けに1回だけ再試行 (それでも失敗したらログのみ = fail-soft。
        // Notion同期は暫定連携で、正本の計測データはpicking.db側にある)
        await new Promise((r) => setTimeout(r, 5000));
        const retryState = getBatchState();
        if (!retryState || !retryState.label) return;
        await syncPickingStatus(retryState.folderName, retryState.label, retryState.workDate);
      }
    })
    .catch((e) => console.warn(`[picking-notion] 同期失敗 (batch=${batchId}): ${e.message}`));
  _batchQueues.set(batchId, next);
  // チェーンが伸び続けないよう、完了時に自分が最後尾なら片付ける
  next.finally(() => { if (_batchQueues.get(batchId) === next) _batchQueues.delete(batchId); });
}
