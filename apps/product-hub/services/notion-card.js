/**
 * product-hub → Notion 商品マスター DB カード自動作成 (要件定義 §5、最優先機能)。
 *
 * 設計:
 *   - Notion client は RYS の実績あるものを流用 (rate limit 350ms + 429/5xx retry 内蔵)。
 *     env も RYS と共通: RYS_NOTION_TOKEN / NOTION_PRODUCT_MASTER_DB_ID (token 流用は中原さん決定 2026-07-04)
 *   - fail-retry 方式: Notion 失敗でもドラフト登録は成功のまま。notion_card_status で追跡し、
 *     画面バナー + リトライで最終的に必ず作る
 *   - 冪等: (1) DB CAS で二重実行防止 (2) 作成前に 商品コード で Notion pre-check、既存なら採用
 *   - status 遷移: pending → creating → created / failed。creating のまま
 *     STALE_CREATING_MS 経過したものは crash 扱いでリトライ対象に戻す
 */
import { createPage, findPageByManageNumber } from '../../rakuten-yahoo-sync/lib/notion-client.js';
import { getDB, logEvent } from '../db.js';

const STALE_CREATING_MS = 10 * 60 * 1000;
const ERROR_MAX_LEN = 500;

function defaultNotionStatus() {
  // RYS のページ自動作成と同じ既定 (新規商品の入口ステータス)。env で差し替え可
  return process.env.PH_NOTION_STATUS_DEFAULT || '⓪新規商品_高島';
}

function buildProperties(draft) {
  const properties = {
    'Name': { title: [{ type: 'text', text: { content: String(draft.name) } }] },
    '商品コード': { rich_text: [{ type: 'text', text: { content: String(draft.ne_code) } }] },
    'Status': { select: { name: defaultNotionStatus() } },
  };
  if (Number.isFinite(draft.price)) {
    properties['売価'] = { number: draft.price };
  }
  // Notion 側 JANコード は number 型 (notion-property.js 準拠)。数値化できない値は送らない
  if (draft.jan_code != null && String(draft.jan_code).trim() !== '') {
    const jan = Number(String(draft.jan_code).trim());
    if (Number.isFinite(jan)) properties['JANコード'] = { number: jan };
  }
  return properties;
}

function truncateError(e) {
  const msg = e && e.message ? String(e.message) : String(e);
  return msg.length > ERROR_MAX_LEN ? `${msg.slice(0, ERROR_MAX_LEN)}…` : msg;
}

/**
 * 1 ドラフトのカード作成を試みる。例外は投げず outcome を返す (呼び出し側 fire-safe)。
 * @returns {{ outcome: string, notionPageId?: string, error?: string }}
 */
export async function attemptCardCreation(draftId, { actor = null } = {}) {
  const db = getDB();
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return { outcome: 'not_found' };
  if (draft.notion_page_id) return { outcome: 'already_created', notionPageId: draft.notion_page_id };

  // CAS claim: pending/failed → creating (creating は stale のときだけ奪取可)
  const staleBefore = new Date(Date.now() - STALE_CREATING_MS).toISOString();
  const claim = db.prepare(`
    UPDATE product_drafts SET
      notion_card_status = 'creating',
      notion_card_attempts = notion_card_attempts + 1,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE id = ?
      AND notion_page_id IS NULL
      AND (notion_card_status IN ('pending','failed')
           OR (notion_card_status = 'creating' AND updated_at < ?))
  `).run(draftId, staleBefore);
  if (claim.changes === 0) return { outcome: 'skip_locked' };

  const finalize = (status, pageId, error) => {
    db.prepare(`
      UPDATE product_drafts SET
        notion_card_status = ?,
        notion_page_id = COALESCE(?, notion_page_id),
        notion_card_error = ?,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE id = ? AND notion_card_status = 'creating'
    `).run(status, pageId || null, error || null, draftId);
  };

  try {
    // pre-check: 既に同じ商品コードのカードがあれば作らず採用 (冪等)
    const existing = await findPageByManageNumber(String(draft.ne_code));
    if (existing?.id) {
      finalize('created', existing.id, null);
      logEvent(db, draftId, 'notion_card_adopted', existing.id, actor);
      return { outcome: 'adopted_existing', notionPageId: existing.id };
    }

    const page = await createPage(buildProperties(draft));
    finalize('created', page.id, null);
    logEvent(db, draftId, 'notion_card_created', page.id, actor);
    return { outcome: 'created', notionPageId: page.id };
  } catch (e) {
    const error = truncateError(e);
    finalize('failed', null, error);
    logEvent(db, draftId, 'notion_card_failed', error, actor);
    return { outcome: 'failed', error };
  }
}

/**
 * 未作成 (pending / failed / stale creating) を全件リトライ。
 * Notion client 側の rate limiter (350ms) が pacing するので直列 await でよい。
 */
export async function retryPendingCards({ actor = null, limit = 50 } = {}) {
  const db = getDB();
  const staleBefore = new Date(Date.now() - STALE_CREATING_MS).toISOString();
  const targets = db.prepare(`
    SELECT id FROM product_drafts
    WHERE notion_page_id IS NULL
      AND (notion_card_status IN ('pending','failed')
           OR (notion_card_status = 'creating' AND updated_at < ?))
    ORDER BY id
    LIMIT ?
  `).all(staleBefore, limit);

  const results = [];
  for (const t of targets) {
    results.push({ draftId: t.id, ...(await attemptCardCreation(t.id, { actor })) });
  }
  return results;
}

/** 画面バナー用: 未作成件数 */
export function pendingCardCount() {
  const db = getDB();
  return db.prepare(`
    SELECT COUNT(*) AS c FROM product_drafts
    WHERE notion_page_id IS NULL AND notion_card_status != 'created'
  `).get().c;
}
