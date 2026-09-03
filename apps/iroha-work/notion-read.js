/**
 * いろは在庫化作業アプリ — Notion「在庫化作業管理」の読み取りとステータス変更
 *
 * v1 は Notion が進捗の正本 (要件定義 §1.5)。このモジュールがやること:
 *   - カード一覧の取得 → f_iroha_app_notion_cache へ全置換 (表示はキャッシュから)
 *   - ステータス変更 = 変更直前に再取得して競合を見てから PATCH → 応答で反映を確認
 *     (Notion に原子的な比較更新は無い。Codex設計相談R2 §2「Notionステータス変更」)
 *
 * 整合性まわり (Codex PR1 レビューで固めた点):
 *   - 変更はページ単位の in-process mutex で直列化 (Render 単一インスタンス前提)。
 *     同じ expect からの同時変更は、後の方が競合として正しく弾かれる (#3)。
 *     ⚠Notion を直接編集する人との競合までは防げない — それは expect 比較が検出する
 *   - 全体取得中にアプリ経由で変えたステータスは recentChanges で覚えておき、
 *     取得開始より後の変更はキャッシュ全置換後に上書きし直す (古い取得結果で巻き戻さない #3)
 *   - ページング上限で取り切れなかったときはキャッシュを**置き換えない** (部分データで
 *     見えていたカードを消さない #4)。truncated を出して古い完全キャッシュを維持する
 *   - スキーマは inbound-check の ensureCardSchema で検証 (ステータス=select 型など。
 *     人が status 型に作り替えたら黙って 400 を撒かず 1回で止まる #5)
 *
 * HTTP 層は inbound-check の notionRequest を共用 (同じインテグレーション・同じ DB。
 * INBOUND_CHECK_NOTION_DB_ID がこの DB の ID)。
 */
import { notionRequest, isNotionConfigured, ensureCardSchema } from '../inbound-check/notion.js';
import { getDB, replaceCache, listCache, updateCacheStatus, upsertCachePage, removeCachePage, getMeta, setMetaValue } from './db.js';

export { isNotionConfigured };

export const STATUSES = ['未着手', '作業中', '中断', '棚入完了'];   // 変更先 (棚入完了は職員のみ)
export const STATUS_DONE = '棚入完了';
const STATUS_CANCELLED = '取消';
// 一覧に取り込まないステータス (中原さん 2026-09-03: 在庫化対象外・作業完了・棚入完了は取り込まない。
// 棚入完了にしたカードはその場で一覧から外れる。完了分は Notion で見る)。
// Notion クエリの除外条件に入れるのは DB スキーマに実在する選択肢だけ (無い名前は 400 になる)、
// 取得後のコード側除外は全部に掛ける (二重の安全)
export const EXCLUDED_STATUSES = [STATUS_CANCELLED, '在庫化対象外', '作業完了', STATUS_DONE];
// 一覧のタブ (= 取り込む状態)。変更先 (STATUSES) とは別物
export const LIST_STATUSES = STATUSES.filter(s => !EXCLUDED_STATUSES.includes(s));
const STATUS_PROP = 'ステータス';

// キャッシュの鮮度。これより古ければ /api/state が自動で取り直す
export const CACHE_FRESH_MS = 3 * 60 * 1000;
// ページング上限 (暴走ガード。超えたら取得を諦めて古い完全キャッシュを守る — 黙って欠けさせない)
const MAX_PAGES_ACTIVE = 10;   // 未完了カード 最大1000枚

function dbId() {
  return process.env.INBOUND_CHECK_NOTION_DB_ID;
}

// ─── ページのパース ───

const plain = (arr) => (Array.isArray(arr) ? arr.map(t => t?.plain_text ?? t?.text?.content ?? '').join('') : '');

/** プロパティ1つを型に応じて素の値へ (未知の型は null)。読み取りは select / status 両対応 */
function propValue(p) {
  if (!p || typeof p !== 'object') return null;
  switch (p.type) {
    case 'title': return plain(p.title) || null;
    case 'rich_text': return plain(p.rich_text) || null;
    case 'select': return p.select?.name ?? null;
    case 'status': return p.status?.name ?? null;
    case 'number': return p.number ?? null;
    case 'date': return p.date?.start ?? null;
    case 'url': return p.url ?? null;
    case 'checkbox': return p.checkbox ?? null;
    default: return null;
  }
}

/** Notion ページ → キャッシュ行。props にはパースできた全プロパティが素の値で入る */
export function parsePage(page) {
  const props = {};
  for (const [name, p] of Object.entries(page.properties || {})) {
    const v = propValue(p);
    if (v != null && v !== '') props[name] = v;
  }
  return {
    pageId: page.id,
    // ステータス未設定のカードは「未着手」として扱う (手で作ったカードにあり得る)
    status: props[STATUS_PROP] || STATUSES[0],
    title: props['名前'] || '(名称なし)',
    productCode: props['商品コード'] || null,
    dedupeKey: props['台帳キー'] || null,
    url: page.url || null,
    lastEditedTime: page.last_edited_time || null,
    props,
  };
}

/** Notion DB を filter でページング取得 (移行ツールからも使う)。truncated = 上限で打ち切り */
export async function queryPages(filter, maxPages) {
  const results = [];
  let cursor = null;
  for (let i = 0; i < maxPages; i++) {
    const body = { page_size: 100 };
    if (filter) body.filter = filter;
    if (cursor) body.start_cursor = cursor;
    const r = await notionRequest(`/databases/${dbId()}/query`, 'POST', body);
    results.push(...(r.results || []));
    if (!r.has_more || !r.next_cursor) return { results, truncated: false };
    cursor = r.next_cursor;
  }
  // ページ上限を使い切ってもまだ続きがある = 取り切れていない
  return { results, truncated: true };
}

// 取得中にアプリ経由で変えたページ (pageId → { row: parsePage結果, at })。
// 全置換が古い取得結果で変更を巻き戻さない・行ごと消さないための覚え書き (Codex PR1 #3 / R2 #1)。
// ⭐UPDATE でなく行まるごと upsert する: 取得の最中に 棚入完了→作業中 と変えると、
//   未完了クエリ (変更前に実行) にも完了クエリ (変更後に実行) にも入らず、行が消えるため
const recentChanges = new Map();

/** パース済みの行をキャッシュに反映する。取り込まないステータス (棚入完了など) なら行を消す = 一覧から外す */
function applyToCache(row, pageId = row?.pageId) {
  if (!row || EXCLUDED_STATUSES.includes(row.status)) removeCachePage(pageId);
  else upsertCachePage(row);
}

/**
 * カード一覧を Notion から取得してキャッシュを全置換する。
 *   除外ステータス (EXCLUDED_STATUSES = 棚入完了・取消・在庫化対象外・作業完了) 以外は**全件** —
 *   何ヶ月放置の未着手も消さない。完了分は取り込まない (中原さん 2026-09-03)。
 * アーカイブ済みページは query に元々返らない。
 * 取り切れなかった (truncated) ときは置き換えず、古い完全キャッシュを守る。
 */
export async function refreshFromNotion() {
  // スキーマ検証 (10分キャッシュ)。ステータスが select 型でなくなった等は 1回のエラーで止める
  const schema = await ensureCardSchema();
  const startedAt = Date.now();
  // ⚠ 除外ステータスは DB スキーマに**実在する選択肢だけ**クエリに入れる: Notion の select フィルタは
  //   存在しない選択肢名を指定すると 400 (select option "取消" not found) になり、一覧が丸ごと
  //   取れなくなる。「取消」は inbound-check が初めて取消を反映した時に自動作成される選択肢で、
  //   それまで存在しない (2026-09-03 実機で判明: キャッシュ 0 枚)。実在しないものは取得後に除外。
  //   棚入完了だけは運用上必ず存在する (無いとアプリが成立しない) ので無条件に入れる
  const known = schema?.selectOptions?.get(STATUS_PROP) || new Set();
  const activeFilter = {
    and: [
      { property: STATUS_PROP, select: { does_not_equal: STATUS_DONE } },
      ...EXCLUDED_STATUSES.filter(s => s !== STATUS_DONE && known.has(s))
        .map(s => ({ property: STATUS_PROP, select: { does_not_equal: s } })),
    ],
  };
  const a = await queryPages(activeFilter, MAX_PAGES_ACTIVE);
  if (a.truncated) {
    setMetaValue('truncated', '1');
    console.warn(`[iroha-work] Notion 取得がページ上限で打ち切り → キャッシュは置き換えない (${a.results.length}件)`);
    return { count: null, truncated: true };
  }
  const seen = new Set();
  const pages = [];
  for (const page of a.results) {
    if (seen.has(page.id)) continue;
    seen.add(page.id);
    const row = parsePage(page);
    if (EXCLUDED_STATUSES.includes(row.status)) continue;   // クエリに入れられなかった分もここで落とす (上記)
    pages.push(row);
  }
  replaceCache(pages);
  // 取得開始より後にアプリで変えたページは、置換結果 (古い) より新しい → 行ごと戻し入れる
  // (棚入完了にした分は applyToCache が消す)。取得開始より前の変更は Notion 側の取得結果に
  // 含まれているので覚え書きを消す
  for (const [pageId, ch] of recentChanges) {
    if (ch.at >= startedAt) applyToCache(ch.row, pageId);
    else recentChanges.delete(pageId);
  }
  setMetaValue('truncated', null);
  return { count: pages.length, truncated: false };
}

// 同時に何度も取りに行かない (画面を2枚開いた・連打などで API を連打しない)
let inflight = null;

/**
 * 必要ならキャッシュを更新して結果を返す。
 * 失敗してもキャッシュは残す (古い一覧 + エラー表示で現場を止めない — Codex R2 ①)。
 * 鮮度の判定は「最後に試みた時刻」(last_attempt_at) — 失敗やカード超過のたびに
 * 全アクセスが Notion を叩き直さないようにする。表示用の last_refresh_at とは別
 * @returns {{ fresh: boolean, error: string|null, lastRefreshAt: string|null, truncated: boolean }}
 */
export async function ensureFresh({ force = false } = {}) {
  const status = () => ({
    lastRefreshAt: getMeta('last_refresh_at'),
    error: getMeta('last_refresh_error'),
    truncated: getMeta('truncated') === '1',
  });
  if (!isNotionConfigured()) {
    return { fresh: false, ...status(), error: 'Notion 連携が未設定です (NOTION_TOKEN / INBOUND_CHECK_NOTION_DB_ID)' };
  }
  const attempted = getMeta('last_attempt_at');
  if (!force && attempted && Date.now() - Date.parse(attempted) < CACHE_FRESH_MS) {
    return { fresh: !status().error, ...status() };
  }
  if (!inflight) {
    inflight = refreshFromNotion()
      .then((r) => {
        setMetaValue('last_refresh_error', null);
        return { ok: !r.truncated };
      })
      .catch((e) => {
        setMetaValue('last_refresh_error', e.message);
        return { ok: false, message: e.message };
      })
      .finally(() => {
        setMetaValue('last_attempt_at', new Date().toISOString());
        inflight = null;
      });
  }
  const r = await inflight;
  return { fresh: r.ok, ...status(), ...(r.ok ? { error: null } : {}) };
}

// ─── ステータス変更 (一覧・詳細から) ───

// ページ単位の直列化 (Codex PR1 #3)。チェーンが終わったらエントリを掃除する
const pageLocks = new Map();
function withPageLock(pageId, fn) {
  const prev = pageLocks.get(pageId) || Promise.resolve();
  const run = prev.then(() => fn());
  const tail = run.then(() => {}, () => {});
  pageLocks.set(pageId, tail);
  tail.then(() => { if (pageLocks.get(pageId) === tail) pageLocks.delete(pageId); });
  return run;
}

/**
 * ステータスを変える。手順 (Codex R2 §2):
 *   ①直前にページを再取得 → 消えていれば card_gone / 今の値が expect と違えば conflict
 *   ②PATCH (select は存在しない選択肢名でも Notion が自動作成する)
 *   ③応答のステータスが to になっているか確認 (HTTP 成功だけを成功と見なさない)
 * expect は必須 (省略で競合検出を素通りさせない — Codex PR1 #2)。
 * 棚入完了への変更・棚入完了からの変更は職員のみ (要件定義 §1.7 ⑤)。isStaff の真偽は
 * 呼び元 (router) が本人確認 (ポータルセッション or 職員PIN) 済みの場合だけ true にする。
 *
 * @returns {ok:true, status} | {ok:false, error, message, current?}
 */
export function changeStatus({ pageId, to, expect, isStaff = false }) {
  return withPageLock(pageId, () => changeStatusLocked({ pageId, to, expect, isStaff }));
}

async function changeStatusLocked({ pageId, to, expect, isStaff }) {
  if (!STATUSES.includes(to)) return { ok: false, error: 'bad_status', message: '変更先のステータスが不正です' };
  if (!expect || typeof expect !== 'string') {
    return { ok: false, error: 'bad_request', message: '変更前のステータス (expect) が必要です。画面を更新してからやり直してください' };
  }

  let page;
  try {
    page = await notionRequest(`/pages/${pageId}`, 'GET');
  } catch (e) {
    if (e.status === 404) {
      removeCachePage(pageId);
      return { ok: false, error: 'card_gone', message: 'このカードは Notion 側で削除されています。一覧を更新します' };
    }
    return { ok: false, error: 'notion_error', message: `Notion に接続できませんでした (${e.message})` };
  }
  if (page.archived) {
    removeCachePage(pageId);
    return { ok: false, error: 'card_gone', message: 'このカードは Notion 側でアーカイブされています。一覧を更新します' };
  }
  // 対象 DB のページであることを確認 (Codex PR1-R2 #2: pageId は端末から自由に送れるので、
  // 同じインテグレーションが触れる**別DB**のページを書き換えられないようにする)
  const parentId = String(page.parent?.database_id || '').replace(/-/g, '').toLowerCase();
  if (parentId !== String(dbId() || '').replace(/-/g, '').toLowerCase()) {
    return { ok: false, error: 'wrong_database', message: 'このページは在庫化作業管理のカードではありません' };
  }
  const stProp = page.properties?.[STATUS_PROP];
  if (stProp && stProp.type !== 'select') {
    return { ok: false, error: 'schema_mismatch',
      message: `Notion の「${STATUS_PROP}」プロパティが select 型ではありません (${stProp.type})。管理者に連絡してください` };
  }
  const current = propValue(stProp) || STATUSES[0];
  if (current === to) {
    // もう目的の状態 (他の端末で同じ変更が済んでいた)。エラーにしない
    updateCacheStatus(pageId, current, page.last_edited_time);
    if (EXCLUDED_STATUSES.includes(current)) removeCachePage(pageId);   // 他端末/Notion で完了済みなら一覧から外す
    return { ok: true, status: current, already: true, listed: !EXCLUDED_STATUSES.includes(current) };
  }
  if (current !== expect) {
    updateCacheStatus(pageId, current, page.last_edited_time);
    if (EXCLUDED_STATUSES.includes(current)) removeCachePage(pageId);   // 他端末/Notion で完了済みなら一覧から外す
    return { ok: false, error: 'conflict', current, listed: !EXCLUDED_STATUSES.includes(current),
      message: `Notion 側で「${current}」に変更されています。最新の状態を表示します` };
  }
  if ((to === STATUS_DONE || current === STATUS_DONE) && !isStaff) {
    return { ok: false, error: 'staff_required',
      message: `「${STATUS_DONE}」への変更・取り消しは職員のみです (職員の名前を選び、PINを入れてください)` };
  }

  let updated;
  try {
    updated = await notionRequest(`/pages/${pageId}`, 'PATCH', {
      properties: { [STATUS_PROP]: { select: { name: to } } },
    });
  } catch (e) {
    return { ok: false, error: 'notion_error', message: `変更できませんでした (${e.message})。もう一度お試しください` };
  }
  const after = propValue(updated.properties?.[STATUS_PROP]);
  if (after !== to) {
    return { ok: false, error: 'verify_failed',
      message: '変更を送りましたが、Notion 側の値が確認できませんでした。一覧を更新して確かめてください' };
  }
  // PATCH 応答はページ全体なので、行まるごとキャッシュへ反映する
  const parsed = parsePage(updated);
  applyToCache(parsed);
  recentChanges.set(pageId, { row: parsed, at: Date.now() });
  // listed=false = 一覧に残らない変更 (棚入完了)。画面はこれで行を外す
  return { ok: true, status: to, listed: !EXCLUDED_STATUSES.includes(to) };
}

/**
 * 1ページの現在状態を Notion から直接取る (キャッシュが古いときの開始可否判定用 — Codex PR2-R2 P1)。
 * changeStatus と同じ検査 (404/アーカイブ/別DB) を通す。
 * @returns {ok:true, status, row} | {ok:false, error, message}
 */
export async function fetchCardLive(pageId) {
  let page;
  try {
    page = await notionRequest(`/pages/${pageId}`, 'GET');
  } catch (e) {
    if (e.status === 404) {
      removeCachePage(pageId);
      return { ok: false, error: 'card_gone', message: 'このカードは Notion 側で削除されています。一覧を更新します' };
    }
    return { ok: false, error: 'notion_error', message: `Notion に接続できませんでした (${e.message})` };
  }
  if (page.archived) {
    removeCachePage(pageId);
    return { ok: false, error: 'card_gone', message: 'このカードは Notion 側でアーカイブされています。一覧を更新します' };
  }
  const parentId = String(page.parent?.database_id || '').replace(/-/g, '').toLowerCase();
  if (parentId !== String(dbId() || '').replace(/-/g, '').toLowerCase()) {
    return { ok: false, error: 'wrong_database', message: 'このページは在庫化作業管理のカードではありません' };
  }
  const parsed = parsePage(page);
  applyToCache(parsed);
  return { ok: true, status: parsed.status, row: parsed };
}

/** 管理画面用のキャッシュ統計 */
export function cacheStatsForAdmin() {
  const db = getDB();
  const total = db.prepare('SELECT COUNT(*) c FROM f_iroha_app_notion_cache').get().c;
  const byStatus = db.prepare('SELECT status, COUNT(*) c FROM f_iroha_app_notion_cache GROUP BY status ORDER BY c DESC').all();
  return {
    total, byStatus,
    lastRefreshAt: getMeta('last_refresh_at'),
    lastRefreshError: getMeta('last_refresh_error'),
    truncated: getMeta('truncated') === '1',
    configured: isNotionConfigured(),
  };
}

export { listCache };
