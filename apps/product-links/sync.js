/**
 * product-hub → 商品リンク台帳 の自動同期。
 *
 * 1 ドラフト分を「いまの値」から冪等に写す (差分計算をしない = 保存フックと夜間照合が同じ関数)。
 *   - product_drafts.drive_folder_url        → link_type=drive_folder
 *   - draft_image_production.canva_url       → link_type=canva (purpose は既存を尊重・新規は NULL のまま人が付ける)
 * 由来 = source_system='product_hub', source_entity_id=draft_id。
 * ドラフト側で URL が変わった / 空になったときは、この由来からは外し、他由来が無ければ論理削除 (detachStaleSources)。
 *
 * 呼び出し側 (product-hub の保存 API) は自分のトランザクションの中から呼ぶ。ここで例外を投げると保存ごと
 * 巻き戻るので、台帳側の失敗は握って記録するだけにする (夜間照合で自己修復する)。
 */
import { upsertLink, detachStaleSources, normalizeCode } from './db.js';

function tableExists(db, name) {
  return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
}

/** 返す: { ok, upserted: number, detached: number } */
export function syncDraftLinks(db, draftId, { actor = 'auto:product_hub' } = {}) {
  try {
    if (!tableExists(db, 'ph_product_links')) return { ok: false, error: 'table_missing' };
    // 1 ドラフト分は原子的に (外側のトランザクションの中では SAVEPOINT になる)。
    // 失敗はここで握る = product-hub の保存を台帳の都合で落とさない (夜間照合が自己修復)
    return db.transaction(() => {
      const draft = db.prepare('SELECT id, ne_code, name, drive_folder_url, updated_at FROM product_drafts WHERE id = ?').get(draftId);
      const keep = [];
      if (draft && normalizeCode(draft.ne_code)) {
        const ip = tableExists(db, 'draft_image_production')
          ? db.prepare('SELECT canva_url, updated_at FROM draft_image_production WHERE draft_id = ?').get(draftId)
          : null;
        const base = { neCode: draft.ne_code, productName: draft.name, source: 'product_hub', sourceEntityId: String(draft.id), createdBy: actor };
        if (draft.drive_folder_url && /^https?:\/\//i.test(draft.drive_folder_url)) {
          keep.push(upsertLink(db, { ...base, linkType: 'drive_folder', url: draft.drive_folder_url, sourceUpdatedAt: draft.updated_at }).id);
        }
        if (ip?.canva_url && /^https?:\/\//i.test(ip.canva_url)) {
          keep.push(upsertLink(db, { ...base, linkType: 'canva', url: ip.canva_url, sourceUpdatedAt: ip.updated_at }).id);
        }
      }
      const detached = detachStaleSources(db, { source: 'product_hub', sourceEntityId: String(draftId), keepLinkIds: keep });
      return { ok: true, upserted: keep.length, detached };
    })();
  } catch (e) {
    console.error(`[product-links] sync draft ${draftId} failed:`, e.message);
    return { ok: false, error: e.message };
  }
}

/**
 * 全ドラフトの照合 (夜間 + 初回バックフィル)。product_hub 由来しか触らない。
 * ドラフトが消えた由来 (entity が product_drafts に無い) も detach する。
 */
export function reconcileAll(db, { actor = 'cron:product-links' } = {}) {
  if (!tableExists(db, 'product_drafts')) return { ok: false, error: 'product_drafts_missing' };
  const ids = db.prepare('SELECT id FROM product_drafts').all().map((r) => r.id);
  const gone = db.prepare(`
    SELECT DISTINCT source_entity_id FROM ph_product_link_sources
    WHERE source_system = 'product_hub' AND detached_at IS NULL
      AND CAST(source_entity_id AS INTEGER) NOT IN (SELECT id FROM product_drafts)
  `).all().map((r) => r.source_entity_id);
  // ドラフト単位の部分成功を許す (1 件の失敗で全件を巻き戻さない)。原子性はドラフト単位 = syncDraftLinks 内
  let upserted = 0; let detached = 0; let failed = 0;
  for (const id of ids) {
    const r = syncDraftLinks(db, id, { actor });
    if (!r.ok) { failed++; continue; }
    upserted += r.upserted; detached += r.detached;
  }
  for (const entity of gone) {
    try { detached += db.transaction(() => detachStaleSources(db, { source: 'product_hub', sourceEntityId: entity, keepLinkIds: [] }))(); }
    catch (e) { failed++; console.error(`[product-links] detach gone draft ${entity} failed:`, e.message); }
  }
  return { ok: failed === 0, drafts: ids.length, upserted, detached, gone: gone.length, failed };
}
