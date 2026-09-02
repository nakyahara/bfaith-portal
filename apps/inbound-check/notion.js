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
 * ⭐カードには必ず「台帳キー」(rich_text, 例 d123-a1b2c3) を入れる。
 *   「ページ作成は成功したが応答を受け取る前に落ちて page_id を保存できなかった」とき、
 *   このキーで既存カードを探して回収する (二重カードの防止)。
 *   行IDそのもの (destination_id, number) は人が見るための表示用 — DB を作り直すと
 *   AUTOINCREMENT が 1 から振り直され過去カードと衝突するため、回収キーには使わない (Codex R1 #8)。
 */

const API_BASE = 'https://api.notion.com/v1';
const NOTION_VERSION = '2022-06-28';
export const DEDUPE_PROP = '台帳キー';

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
export async function notionRequest(path, method, body, { maxAttempts = 3 } = {}) {
  const { token } = getConfig();
  const MAX = Math.max(1, maxAttempts);
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
      // Retry-After は上限 30 秒で丸める (異常値で sweep 全体が長時間止まらないように — Codex R1 #11)
      await sleep(ra > 0 ? Math.min(ra * 1000, 30_000) : backoff(attempt));
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

// プロセス内キャッシュ。プロパティ構成はほぼ変わらないので毎 sweep 取り直さない
let schemaCache = null; // { at: ms, schema: { names: Set, types: Map } }
const SCHEMA_TTL_MS = 10 * 60 * 1000;

/**
 * カード DB のプロパティを確認し、無ければ足す:
 *   台帳キー (rich_text)     … 回収用の一意キー (必須)
 *   destination_id (number)  … 台帳の行ID (人が見る表示用)
 *   有効期限 (rich_text)     … YYYY-MM-DD / YYYY-MM の両方が入るので date 型にしない
 * 既存プロパティ (名前・ステータス・商品コード…) は一切触らない。
 *
 * ⭐この経路が依存するプロパティの「型」も検証する (Codex R1 #9)。
 *   型が合わないと全行が個別に 400 でブロックされるので、スキーマ全体の問題として
 *   1回だけ NOTION_SCHEMA_MISMATCH で失敗させる (行にはエラーを書かない)。
 *
 * @returns {{ names: Set<string>, types: Map<string,string> }}
 */
export async function ensureCardSchema({ force = false } = {}) {
  const { dbId } = getConfig();
  if (!force && schemaCache && Date.now() - schemaCache.at < SCHEMA_TTL_MS) return schemaCache.schema;
  const db = await notionRequest(`/databases/${dbId}`, 'GET');
  const names = new Set(Object.keys(db.properties || {}));
  const types = new Map(Object.entries(db.properties || {}).map(([k, v]) => [k, v?.type]));

  const add = {};
  if (!names.has(DEDUPE_PROP)) add[DEDUPE_PROP] = { rich_text: {} };
  if (!names.has('destination_id')) add['destination_id'] = { number: {} };
  if (!names.has('有効期限')) add['有効期限'] = { rich_text: {} };
  // いろは作業仕様マスタ (f_iroha_work_master) 由来の項目も、無ければ作る (Codex PR2 #5:
  // names に無いと黙って送られず、新しい環境で一部欠落したまま正常終了してしまう)
  if (!names.has('工程数')) add['工程数'] = { number: {} };
  // 資材セットID・収納容器は実DBが select 型 (2026-09-02 実機で判明。GAS はこの2項目を
  // 実際には送っていなかったため型の思い込みが露見しなかった)
  if (!names.has('資材セットID')) add['資材セットID'] = { select: {} };
  if (!names.has('収納容器')) add['収納容器'] = { select: {} };
  if (!names.has('入数')) add['入数'] = { number: {} };
  if (!names.has('備考')) add['備考'] = { rich_text: {} };
  if (Object.keys(add).length > 0) {
    await notionRequest(`/databases/${dbId}`, 'PATCH', { properties: add });
    for (const [k, v] of Object.entries(add)) { names.add(k); types.set(k, Object.keys(v)[0]); }
  }

  // 型の検証 (存在するのに型が違う = 人が作り替えた等)。1項目でも合わなければ、行ごとに
  // 400 を書き散らす前にスキーマ全体の問題として1回で止める (Codex R1 #9 / R2 #3)。
  // ⭐この一覧 = この経路が**送る全プロパティ**。送る項目を増やしたらここにも足すこと
  const EXPECTED_TYPES = {
    '名前': 'title', 'ステータス': 'select', '商品コード': 'rich_text', '数量': 'number',
    '入庫日': 'date', '入荷管理番号': 'rich_text', 'バーコード': 'rich_text', '取引先': 'select',
    '仕入先': 'number', '取扱区分': 'select', '在庫化必要FLG': 'checkbox', '作業拠点': 'select',
    '過去30日販売数': 'rich_text', '外部出し目安': 'rich_text',
    '資材セットID': 'select', '収納容器': 'select', '入数': 'number', '工程数': 'number', '備考': 'rich_text',
    '有効期限': 'rich_text', 'destination_id': 'number', [DEDUPE_PROP]: 'rich_text',
  };
  const problems = [];
  for (const [k, t] of Object.entries(EXPECTED_TYPES)) {
    if (names.has(k) && types.get(k) !== undefined && types.get(k) !== t) {
      problems.push(`「${k}」は ${t} 型が必要 (現在 ${types.get(k)})`);
    }
  }
  // 名前 (title) とステータスは無いと成立しない (名前が無いと全行が落ち、ステータスが無いと取消を反映できない)
  for (const requiredName of ['名前', 'ステータス']) {
    if (!names.has(requiredName)) problems.push(`「${requiredName}」プロパティが見つかりません (改名した場合は元に戻してください)`);
  }
  if (problems.length > 0) {
    const e = new Error('Notion DB のプロパティが想定と違います: ' + problems.join(' / '));
    e.code = 'NOTION_SCHEMA_MISMATCH';
    throw e;
  }
  const schema = { names, types };
  schemaCache = { at: Date.now(), schema };
  return schema;
}

/** テストからキャッシュを消すため */
export function _clearSchemaCache() { schemaCache = null; }

// ─── ページ操作 ───

/**
 * 台帳キーでカードを検索 (回収用)。複数返る = 二重カードの検出も兼ねる。
 * ⚠has_more/next_cursor で**全件**取り切る (件数固定だと4枚目以降の二重カードが
 *   取消されず有効なまま残る — Codex R4 High)。通常は 0〜1 件で1ページで終わる。
 *   10ページ (200件) で打ち切るのは暴走ガード — そこまで増える事故は別問題として表面化させる
 */
export async function findCardsByDedupeKey(dedupeKey) {
  const { dbId } = getConfig();
  const results = [];
  let cursor = null;
  for (let page = 0; page < 10; page++) {
    const body = {
      filter: { property: DEDUPE_PROP, rich_text: { equals: String(dedupeKey) } },
      page_size: 20,
    };
    if (cursor) body.start_cursor = cursor;
    const r = await notionRequest(`/databases/${dbId}/query`, 'POST', body);
    results.push(...(r.results || []));
    if (!r.has_more || !r.next_cursor) break;
    cursor = r.next_cursor;
  }
  return results;
}

/**
 * カードを1枚作成 → { id, url }。
 * ⚠POST は **1回だけ** (HTTP 層の自動再試行なし — Codex R2 #1)。
 *   タイムアウト/5xx は「Notion 側では作成成功したが応答が消えた」可能性があり、
 *   盲目的に再POSTすると2枚目ができる。曖昧な失敗のやり直しは呼び元 (notion-sync の
 *   createCardSafe) が台帳キーで再検索してから行う
 */
export async function createCard(properties) {
  const { dbId } = getConfig();
  const data = await notionRequest('/pages', 'POST', {
    parent: { database_id: dbId },
    properties,
  }, { maxAttempts: 1 });
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
