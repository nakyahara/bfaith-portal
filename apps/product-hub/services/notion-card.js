/**
 * product-hub → Notion 商品マスター DB カード自動作成 (要件定義 §5、最優先機能)。
 *
 * 設計:
 *   - Notion client は RYS の実績あるものを流用 (rate limit 350ms + 429/5xx retry 内蔵)。
 *     env も RYS と共通: RYS_NOTION_TOKEN / NOTION_PRODUCT_MASTER_DB_ID (token 流用は中原さん決定 2026-07-04)
 *   - fail-retry 方式: Notion 失敗でもドラフト登録は成功のまま。notion_card_status で追跡し、
 *     画面バナー + リトライで最終的に必ず作る
 *   - 冪等: (1) DB CAS + claim token で二重実行防止 (2) 作成前に 商品コード で Notion pre-check、既存なら採用
 *   - status 遷移: pending → creating → created / failed。creating のまま
 *     STALE_CREATING_MS 経過したものは crash 扱いでリトライ対象に戻す
 *   - claim token (Codex R1 medium 対応): stale 奪取と旧 worker が競合しても、
 *     createPage 直前の claim 再確認 + finalize の claim 一致 CAS で二重カードを防ぐ
 */
import crypto from 'crypto';

import { createPage, findPageByManageNumber, patchPageProperties } from '../../rakuten-yahoo-sync/lib/notion-client.js';
import { getDB, logEvent, canWriteToNotion } from '../db.js';

// notion-client の worst case (timeout 30s × retry 5 + backoff) を大きく上回る保守的な値
const STALE_CREATING_MS = 30 * 60 * 1000;
const ERROR_MAX_LEN = 500;

/**
 * Notion カード連携を行うか。**既定 OFF** (2026-07-25 中原さん決定: Notion を切る)。
 * 中原さんが Notion 側の「新商品が入ってきたらカードを作成する処理」を止めたため、
 * アプリからも作らない。コードは残してあるので env で戻せる。
 * ⚠️ OFF の間、RYS は Notion 経由で Yahoo 項目を得られない = 新商品は Yahoo に出せない。
 *    Yahoo 展開には要件定義 §12 のアダプタ (draft_yahoo → RYS) が別途必要。
 */
const ON = new Set(['1', 'true', 'on', 'yes']);
export function isNotionCardEnabled() {
  return ON.has(String(process.env.PH_NOTION_CARD_ENABLED ?? '').trim().toLowerCase());
}

function defaultNotionStatus() {
  // RYS のページ自動作成と同じ既定 (新規商品の入口ステータス)。env で差し替え可
  return process.env.PH_NOTION_STATUS_DEFAULT || '⓪新規商品_高島';
}

export function buildProperties(draft) {
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
  // URL 系 (notion-schema.json 実測: どちらも url 型)。公式ページ = メーカーページURL
  if (draft.official_url) {
    properties['メーカーページURL'] = { url: draft.official_url };
  }
  if (draft.amazon_url) {
    properties['amazon販売ページ'] = { url: draft.amazon_url };
  }
  return properties;
}

/**
 * 既存カードへ URL 項目だけを再同期する (基本情報の保存時に呼ぶ)。
 * URL はポータル起点の項目なので上書きしてよい。売価/JAN 等は Notion 側が
 * RYS 運用の正データ (人が編集する) のため、作成時のみ設定し更新では触らない。
 * fail-soft: 失敗しても保存自体は成功のまま。イベントに記録して返す。
 */
export async function syncCardLinks(draftId, { actor = null } = {}) {
  const db = getDB();
  if (!isNotionCardEnabled()) return { outcome: 'disabled' };
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return { outcome: 'not_found' };
  // fail-closed: portal 起点だけ書き戻す。取り込み由来 (Notion が正) はここで抜ける
  if (!canWriteToNotion(draft)) return { outcome: 'skipped_not_portal' };
  if (!draft.notion_page_id) return { outcome: 'no_card' };

  // 空値は送らない。#423 は「ポータルで消したら Notion も消す」意図で { url: null } を
  // 送っていたが、既存カードを採用/取り込みした行で未入力のまま保存すると実データが
  // 消える (Codex R1 critical)。クリアの利便より破壊の回避を優先し、非空だけ送る。
  const props = {};
  if (draft.official_url) props['メーカーページURL'] = { url: draft.official_url };
  if (draft.amazon_url) props['amazon販売ページ'] = { url: draft.amazon_url };
  if (Object.keys(props).length === 0) return { outcome: 'nothing_to_sync' };

  try {
    // TOCTOU 縮小 (Codex R1 high-2): API 直前に source を読み直す。
    // DB を共有する別プロセスや将来の機能が source を変えても、書き込み直前で弾く
    const fresh = db.prepare('SELECT source, notion_page_id FROM product_drafts WHERE id = ?').get(draftId);
    if (!canWriteToNotion(fresh) || fresh.notion_page_id !== draft.notion_page_id) {
      return { outcome: 'skipped_not_portal' };
    }
    // 保存レスポンスを道連れにしないよう短い timeout + リトライ1回に制限 (Codex medium)。
    // 失敗しても fail-soft でイベントに残るだけなので、粘るより早く返す方が UX が良い
    await patchPageProperties(draft.notion_page_id, props, { cfg: { timeoutMs: 10_000 }, maxRetries: 1 });
    logEvent(db, draftId, 'notion_card_links_synced', null, actor);
    return { outcome: 'synced' };
  } catch (e) {
    const error = truncateError(e);
    logEvent(db, draftId, 'notion_card_sync_failed', error, actor);
    return { outcome: 'failed', error };
  }
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
  if (!isNotionCardEnabled()) return { outcome: 'disabled' };
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return { outcome: 'not_found' };
  // fail-closed: portal 起点だけ作成対象。取り込み由来は既に Notion にカードが在る (取り込み元がそれ)
  if (!canWriteToNotion(draft)) return { outcome: 'skipped_not_portal' };
  if (draft.notion_page_id) return { outcome: 'already_created', notionPageId: draft.notion_page_id };

  // CAS claim: pending/failed → creating (creating は stale のときだけ奪取可)。
  // claim token で「どの試行がロックを持っているか」を識別する
  const claimToken = crypto.randomUUID();
  const staleBefore = new Date(Date.now() - STALE_CREATING_MS).toISOString();
  const claim = db.prepare(`
    UPDATE product_drafts SET
      notion_card_status = 'creating',
      notion_card_claim = ?,
      notion_card_attempts = notion_card_attempts + 1,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE id = ?
      AND source = 'portal'
      AND notion_page_id IS NULL
      AND ne_code = ?
      AND (notion_card_status IN ('pending','failed')
           OR (notion_card_status = 'creating' AND updated_at < ?))
  `).run(claimToken, draftId, draft.ne_code, staleBefore);
  if (claim.changes === 0) return { outcome: 'skip_locked' };

  // claim 成立後に読み直す。ここまでの間に regroup で ne_code が変わっていても、
  // claim の CAS (ne_code 一致) が弾くので、この行は「読んだときと同じ商品」であることが保証される。
  // 商品名なども古いスナップショットを使わないよう、以降は claimed を使う (Codex R3 high)
  const claimed = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!claimed) return { outcome: 'not_found' };

  // claim がまだ自分のものか (stale 奪取されていないか) を確認する。
  // source / ne_code も一緒に見る: claim 後に取り込み由来へ変わった行や、
  // 商品コードが付け替わった行へは書き込ませない (Codex R1 high-2 / R3 high)
  const holdsClaim = () => {
    const row = db.prepare('SELECT notion_card_status, notion_card_claim, source, ne_code FROM product_drafts WHERE id = ?').get(draftId);
    return row && row.notion_card_status === 'creating' && row.notion_card_claim === claimToken
      && canWriteToNotion(row) && row.ne_code === claimed.ne_code;
  };

  // finalize は claim 一致時のみ書き込む CAS (奪取した新 worker の結果を旧 worker が上書きしない)。
  // changes === 0 は claim 喪失 (Codex R2 low: 呼び出し側に伝える)
  const finalize = (status, pageId, error) => {
    const info = db.prepare(`
      UPDATE product_drafts SET
        notion_card_status = ?,
        notion_page_id = COALESCE(?, notion_page_id),
        notion_card_error = ?,
        notion_card_claim = NULL,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE id = ? AND notion_card_status = 'creating' AND notion_card_claim = ?
        AND source = 'portal' AND ne_code = ?
    `).run(status, pageId || null, error || null, draftId, claimToken, claimed.ne_code);
    return info.changes > 0;
  };

  try {
    // pre-check: 既に同じ商品コードのカードがあれば作らず採用 (冪等)
    const existing = await findPageByManageNumber(String(claimed.ne_code));
    if (existing?.id) {
      if (!finalize('created', existing.id, null)) return { outcome: 'claim_lost' };
      logEvent(db, draftId, 'notion_card_adopted', existing.id, actor);
      return { outcome: 'adopted_existing', notionPageId: existing.id };
    }

    // createPage 直前に claim 再確認: stale 奪取されていたら作成せず撤退 (二重カード防止)
    if (!holdsClaim()) return { outcome: 'claim_lost' };

    const page = await createPage(buildProperties(claimed));
    if (!finalize('created', page.id, null)) {
      // 作成は成功したが claim を失った (stale 奪取後)。page.id を握ったまま黙らない
      logEvent(db, draftId, 'notion_card_claim_lost_after_create', page.id, actor);
      return { outcome: 'claim_lost_after_create', notionPageId: page.id };
    }
    logEvent(db, draftId, 'notion_card_created', page.id, actor);
    return { outcome: 'created', notionPageId: page.id };
  } catch (e) {
    const error = truncateError(e);
    if (!finalize('failed', null, error)) {
      // claim 喪失後の旧 worker の失敗 — DB は新 worker の結果が正なので上書きしない
      return { outcome: 'claim_lost', error };
    }
    logEvent(db, draftId, 'notion_card_failed', error, actor);
    return { outcome: 'failed', error };
  }
}

/**
 * 未作成 (pending / failed / stale creating) を全件リトライ。
 * Notion client 側の rate limiter (350ms) が pacing するので直列 await でよい。
 */
export async function retryPendingCards({ actor = null, limit = 50 } = {}) {
  if (!isNotionCardEnabled()) return [];
  const db = getDB();
  const staleBefore = new Date(Date.now() - STALE_CREATING_MS).toISOString();
  const targets = db.prepare(`
    SELECT id FROM product_drafts
    WHERE notion_page_id IS NULL
      AND source = 'portal'
      -- 仮コードのセット派生はカードを作らない (canWriteToNotion と条件を揃える。
      -- ここで拾うと毎回 attemptCardCreation が skipped を返して空回りする)
      AND provisional_code = 0
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
  if (!isNotionCardEnabled()) return 0; // 連携OFF中はバナーで急かさない
  const db = getDB();
  return db.prepare(`
    SELECT COUNT(*) AS c FROM product_drafts
    WHERE notion_page_id IS NULL AND notion_card_status != 'created'
      AND source = 'portal'
      AND provisional_code = 0
  `).get().c;
}
