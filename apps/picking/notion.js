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

// DBメタデータ (プロパティ名/型) のキャッシュ。10分TTL —
// Notion側にプロパティを後から足したとき、サービス再起動なしで検出できるように
let _schemaCache = null;
let _schemaCachedAt = 0;
const SCHEMA_TTL_MS = 10 * 60 * 1000;
async function getSchema() {
  if (_schemaCache && Date.now() - _schemaCachedAt < SCHEMA_TTL_MS) return _schemaCache;
  const db = await notionFetch(`/databases/${NOTION_DB_ID}`);
  if (!db) return null;
  let titleProp = null;
  let statusProp = null;
  let workerProp = null;   // 「ピッキング担当者」(select)。無いDBでも動く (担当者連携だけスキップ)
  const optional = {};     // 時間系 (作られているものだけ書き込む)
  const OPTIONAL_PROPS = {
    startProp: { name: 'ピッキング開始', type: 'date' },
    endProp: { name: 'ピッキング終了', type: 'date' },
    minutesProp: { name: 'ピッキング時間(分)', type: 'number' },
    secPerLineProp: { name: '秒/明細', type: 'number' },
  };
  for (const [name, def] of Object.entries(db.properties || {})) {
    if (def.type === 'title') titleProp = name;
    if ((def.type === 'status' || def.type === 'select') && name === 'ステータス') {
      statusProp = { name, type: def.type };
    }
    if (def.type === 'select' && name === 'ピッキング担当者') {
      workerProp = { name, type: def.type };
    }
    for (const [key, spec] of Object.entries(OPTIONAL_PROPS)) {
      if (name === spec.name && def.type === spec.type) optional[key] = name;
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
  _schemaCache = { titleProp, statusProp, workerProp, ...optional };
  _schemaCachedAt = Date.now();
  return _schemaCache;
}

/** UTC ISO → Notion date 用の JST 表記 ('YYYY-MM-DDTHH:mm:00+09:00')。 */
export function toJstDateValue(utcIso) {
  const t = Date.parse(utcIso || '');
  if (!Number.isFinite(t)) return null;
  const d = new Date(t + 9 * 3600 * 1000);
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:00+09:00`;
}

/**
 * PATCH する properties を組み立てる (テスト可能な純関数)。
 * 担当者は「メールアドレスでない表示名」のときだけ設定する
 * (セッションログインの email を select の選択肢に増殖させない)。
 * 時間系プロパティは Notion 側に存在するものだけ書く。完了取消 (再作業中) は
 * 終了・時間を null でクリアし、古い値が残らないようにする。
 * @param times {startedAt, finishedAt, activeSec, lineCount} | null
 */
export function buildCardProperties(schema, statusLabel, workerName, times = null) {
  const props = {
    [schema.statusProp.name]: schema.statusProp.type === 'status'
      ? { status: { name: statusLabel } }
      : { select: { name: statusLabel } },
  };
  if (schema.workerProp && workerName && !workerName.includes('@')) {
    props[schema.workerProp.name] = { select: { name: workerName } };
  }
  if (times) {
    if (schema.startProp && times.startedAt) {
      props[schema.startProp] = { date: { start: toJstDateValue(times.startedAt) } };
    }
    const finished = !!times.finishedAt;
    if (schema.endProp) {
      props[schema.endProp] = { date: finished ? { start: toJstDateValue(times.finishedAt) } : null };
    }
    if (schema.minutesProp) {
      props[schema.minutesProp] = { number: finished && times.activeSec != null ? Math.round(times.activeSec / 60 * 10) / 10 : null };
    }
    if (schema.secPerLineProp) {
      props[schema.secPerLineProp] = {
        number: finished && times.activeSec != null && times.lineCount > 0
          ? Math.round(times.activeSec / times.lineCount * 10) / 10 : null,
      };
    }
  }
  return props;
}

/** Notionの「ピッキング担当者」selectの選択肢一覧 (作業者マスタ取込用)。 */
export async function fetchNotionWorkerNames() {
  const db = await notionFetch(`/databases/${NOTION_DB_ID}`);
  if (!db) return [];
  const prop = db.properties?.['ピッキング担当者'];
  if (!prop || prop.type !== 'select') return [];
  return (prop.select?.options || []).map((o) => o.name).filter(Boolean);
}

/** JST 'YYYY-MM-DD' → その日のUTC区間 [start, end) */
function jstDayRangeUtc(jstDate) {
  const start = new Date(`${jstDate}T00:00:00+09:00`);
  const end = new Date(start.getTime() + 24 * 3600 * 1000);
  return { start: start.toISOString(), end: end.toISOString() };
}

/**
 * タイトルが出荷フォルダ名に対応するか (完全一致ではなく「含む」— 2026-08-13 中原さん指定)。
 * カードは「出荷_18 ネコポス」のような命名もあるため部分一致で拾うが、
 * 単純な contains だと「出荷_1」が「出荷_18」に誤マッチするので番号の境界を見る。
 * 出荷_01 / 出荷_1 のゼロ埋め表記ゆれも同一視する。
 */
export function titleMatchesFolder(title, folderName) {
  const t = String(title || '');
  const m = String(folderName || '').match(/^出荷_0*(\d+)$/);
  if (!m) return t.includes(String(folderName));
  return new RegExp(`出荷_0*${m[1]}(?!\\d)`).test(t);
}

/**
 * 「タイトルに folderName を含む・今日 (JST) 作成」のカードを1枚探す。
 * 複数該当は最新1枚 (作成日時降順の先頭)。
 */
async function findTodayCard(folderName, jstDate) {
  const schema = await getSchema();
  if (!schema) return null;
  // クエリは Notion の contains で広めに取り (ゼロ埋め表記ゆれがあるため番号のみで検索)、
  // 正確な判定は titleMatchesFolder で行う
  const m = String(folderName).match(/^出荷_0*(\d+)$/);
  const q = await notionFetch(`/databases/${NOTION_DB_ID}/query`, {
    method: 'POST',
    body: {
      filter: { property: schema.titleProp, title: { contains: m ? `出荷_` : String(folderName) } },
      sorts: [{ timestamp: 'created_time', direction: 'descending' }],
      page_size: 100,
    },
  });
  if (!q) return null;
  const { start, end } = jstDayRangeUtc(jstDate);
  const titleText = (p) => (p.properties?.[schema.titleProp]?.title || []).map((t) => t.plain_text).join('');
  return (q.results || []).find((p) =>
    p.created_time >= start && p.created_time < end && titleMatchesFolder(titleText(p), folderName)
  ) || null;
}

/**
 * カードのステータスを変更する。対象カードが無ければ何もしない (エラーにしない —
 * Notion側のカード作成が遅れている朝一などに作業を止めないため)。
 * @returns {'updated'|'no_card'|'disabled'}
 */
export async function syncPickingStatus(folderName, statusLabel, jstDate, workerName = null, times = null) {
  if (!process.env.PICKING_NOTION_TOKEN) return 'disabled';
  if (!folderName) return 'no_card';
  const card = await findTodayCard(folderName, jstDate);
  if (!card) {
    console.warn(`[picking-notion] ${jstDate} の「${folderName}」カードが見つかりません (${statusLabel} へ変更なし)`);
    return 'no_card';
  }
  const schema = await getSchema();
  await notionFetch(`/pages/${card.id}`, {
    method: 'PATCH',
    body: { properties: buildCardProperties(schema, statusLabel, workerName, times) },
  });
  console.log(`[picking-notion] ${folderName} → ${statusLabel}${workerName ? ` (担当: ${workerName})` : ''}`);
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
        await syncPickingStatus(state.folderName, state.label, state.workDate, state.workerName, state.times);
      } catch (e) {
        // 一時障害向けに1回だけ再試行 (それでも失敗したらログのみ = fail-soft。
        // Notion同期は暫定連携で、正本の計測データはpicking.db側にある)
        await new Promise((r) => setTimeout(r, 5000));
        const retryState = getBatchState();
        if (!retryState || !retryState.label) return;
        await syncPickingStatus(retryState.folderName, retryState.label, retryState.workDate, retryState.workerName, retryState.times);
      }
    })
    .catch((e) => console.warn(`[picking-notion] 同期失敗 (batch=${batchId}): ${e.message}`));
  _batchQueues.set(batchId, next);
  // チェーンが伸び続けないよう、完了時に自分が最後尾なら片付ける
  next.finally(() => { if (_batchQueues.get(batchId) === next) _batchQueues.delete(batchId); });
}
