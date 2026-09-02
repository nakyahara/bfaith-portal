/**
 * いろは在庫化作業アプリ — Notion「在庫化作業管理」の読み取りとステータス変更
 *
 * v1 は Notion が進捗の正本 (要件定義 §1.5)。このモジュールがやること:
 *   - カード一覧の取得 → f_iroha_app_notion_cache へ全置換 (表示はキャッシュから)
 *   - ステータス変更 = 変更直前に再取得して競合を見てから PATCH → 応答で反映を確認
 *     (Notion に原子的な比較更新は無い。Codex設計相談R2 §2「Notionステータス変更」)
 *
 * HTTP 層は inbound-check の notionRequest を共用 (同じインテグレーション・同じ DB。
 * INBOUND_CHECK_NOTION_DB_ID がこの DB の ID)。
 */
import { notionRequest, isNotionConfigured } from '../inbound-check/notion.js';
import { getDB, replaceCache, listCache, updateCacheStatus, removeCachePage, getMeta, setMetaValue } from './db.js';

export { isNotionConfigured };

export const STATUSES = ['未着手', '作業中', '中断', '棚入完了'];
export const STATUS_DONE = '棚入完了';
const STATUS_CANCELLED = '取消';
const STATUS_PROP = 'ステータス';

// 棚入完了は最近のものだけ一覧に出す (全履歴を毎回引かない。それより古い完了分は Notion で見る)
export const DONE_WINDOW_DAYS = 14;
// キャッシュの鮮度。これより古ければ /api/state が自動で取り直す
export const CACHE_FRESH_MS = 3 * 60 * 1000;
// ページング上限 (暴走ガード。超えたら打ち切りを meta に残して画面へ出す — 黙って欠けさせない)
const MAX_PAGES_ACTIVE = 10;   // 未完了カード 最大1000枚
const MAX_PAGES_DONE = 5;      // 直近の棚入完了 最大500枚

function dbId() {
  return process.env.INBOUND_CHECK_NOTION_DB_ID;
}

// ─── ページのパース ───

const plain = (arr) => (Array.isArray(arr) ? arr.map(t => t?.plain_text ?? t?.text?.content ?? '').join('') : '');

/** プロパティ1つを型に応じて素の値へ (未知の型は null)。ステータスは select / status 両対応 */
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

async function queryPages(filter, maxPages) {
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

/**
 * カード一覧を Notion から取得してキャッシュを全置換する。
 *   ①未完了 (棚入完了・取消以外) は**全件** — 何ヶ月放置の未着手も消さない
 *   ②棚入完了は直近 DONE_WINDOW_DAYS 日に編集されたものだけ
 * アーカイブ済みページは query に元々返らない。
 */
export async function refreshFromNotion() {
  const activeFilter = {
    and: [
      { property: STATUS_PROP, select: { does_not_equal: STATUS_DONE } },
      { property: STATUS_PROP, select: { does_not_equal: STATUS_CANCELLED } },
    ],
  };
  const doneSince = new Date(Date.now() - DONE_WINDOW_DAYS * 24 * 3600 * 1000).toISOString();
  const doneFilter = {
    and: [
      { property: STATUS_PROP, select: { equals: STATUS_DONE } },
      { timestamp: 'last_edited_time', last_edited_time: { on_or_after: doneSince } },
    ],
  };
  const a = await queryPages(activeFilter, MAX_PAGES_ACTIVE);
  const b = await queryPages(doneFilter, MAX_PAGES_DONE);
  const seen = new Set();
  const pages = [];
  for (const page of [...a.results, ...b.results]) {
    if (seen.has(page.id)) continue;
    seen.add(page.id);
    pages.push(parsePage(page));
  }
  replaceCache(pages);
  const truncated = a.truncated || b.truncated;
  setMetaValue('truncated', truncated ? '1' : null);
  if (truncated) console.warn(`[iroha-work] Notion 取得がページ上限で打ち切り (未完了${a.results.length}件/完了${b.results.length}件)`);
  return { count: pages.length, truncated };
}

// 同時に何度も取りに行かない (画面を2枚開いた・連打などで API を連打しない)
let inflight = null;

/**
 * 必要ならキャッシュを更新して結果を返す。
 * 失敗してもキャッシュは残す (古い一覧 + エラー表示で現場を止めない — Codex R2 ①)
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
  const last = getMeta('last_refresh_at');
  if (!force && last && Date.now() - Date.parse(last) < CACHE_FRESH_MS) {
    return { fresh: true, ...status() };
  }
  if (!inflight) {
    inflight = refreshFromNotion()
      .then(() => ({ ok: true }))
      .catch((e) => {
        setMetaValue('last_refresh_error', e.message);
        return { ok: false, message: e.message };
      })
      .finally(() => { inflight = null; });
  }
  const r = await inflight;
  return { fresh: r.ok, ...status(), ...(r.ok ? { error: null } : { error: r.message }) };
}

// ─── ステータス変更 (一覧・詳細から) ───

/**
 * ステータスを変える。手順 (Codex R2 §2):
 *   ①直前にページを再取得 → 消えていれば card_gone / 今の値が expect と違えば conflict
 *   ②PATCH (select は存在しない選択肢名でも Notion が自動作成する)
 *   ③応答のステータスが to になっているか確認 (HTTP 成功だけを成功と見なさない)
 * 棚入完了への変更・棚入完了からの変更は職員のみ (要件定義 §1.7 ⑤)。
 *
 * @returns {ok:true, status} | {ok:false, error, message, current?}
 */
export async function changeStatus({ pageId, to, expect, isStaff = false }) {
  if (!STATUSES.includes(to)) return { ok: false, error: 'bad_status', message: '変更先のステータスが不正です' };

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
  const current = propValue(page.properties?.[STATUS_PROP]) || STATUSES[0];
  if (current === to) {
    // もう目的の状態 (他の端末で同じ変更が済んでいた)。エラーにしない
    updateCacheStatus(pageId, current, page.last_edited_time);
    return { ok: true, status: current, already: true };
  }
  if (expect != null && current !== expect) {
    updateCacheStatus(pageId, current, page.last_edited_time);
    return { ok: false, error: 'conflict', current,
      message: `Notion 側で「${current}」に変更されています。最新の状態を表示します` };
  }
  if ((to === STATUS_DONE || current === STATUS_DONE) && !isStaff) {
    return { ok: false, error: 'staff_required',
      message: `「${STATUS_DONE}」への変更・取り消しは職員の方が行ってください (作業者の選択を職員に切り替えてから)` };
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
  updateCacheStatus(pageId, to, updated.last_edited_time);
  return { ok: true, status: to };
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
