/**
 * Phase E-13 → 再設計 R4 でサービス抽出: 楽天 RMS から rakutenItem を取得し、
 * Notion override の空欄項目 (Yahoo!タイトル / 売価 / 税率) を自動下書きする。
 *
 * 元は router.js の /api/admin/seed-notion-drafts インライン実装 (Codex E-13/E-15 レビュー済)。
 * 「全部更新」パイプライン (refresh-pipeline.js) からも呼ぶため関数化。 ロジックは移動のみ。
 *
 * 補完対象 (Notion 列):
 *   Yahoo!タイトル ← 楽天 title 65 字 truncate
 *   売価           ← 楽天 variants[].standardPrice (最小値)
 *   税率           ← 楽天 payment.taxRate (0.1 → '10%')
 * 配送方法は対象外 (Codex R1 H-1: 楽天 normalDeliveryDateId は配送業者でなく lead time)。
 */

import { fetchItemDetailsBulkDetailed } from '../lib/rakuten-rms-proxy.js';
import { buildNotionDraftProposal, toNotionProperties } from '../lib/rakuten-to-notion-draft.js';
import { patchPageProperties } from '../lib/notion-client.js';

/**
 * @param {object} opts
 * @param {Database} opts.db
 * @param {boolean} [opts.dryRun=true]   true なら PATCH せず proposed/skipped 一覧のみ
 * @param {string[]|null} [opts.itemCodes=null]  省略時は notion_overrides 全件で必須項目空欄あり
 * @param {number} [opts.limit=200]      1 回の最大処理 SKU 数 (max 500)
 * @param {function} [opts.audit]        audit(db, action, detail) 互換 (省略時 no-op)
 * @returns {{ totalScanned, proposed, skipped, applied, errors, details }}
 * @throws 楽天 RMS bulk fetch 自体の失敗は Error (statusCode=502) を throw
 */
export async function seedNotionDrafts({ db, dryRun = true, itemCodes = null, limit = 200, audit = null } = {}) {
  const lim = Math.max(1, Math.min(parseInt(limit, 10) || 200, 500));
  const explicitCodes = Array.isArray(itemCodes) ? itemCodes.map(String).filter(Boolean) : null;

  // 1. 対象 SKU 取得 (notion_overrides に居て、 yahoo_title/yahoo_price/notion_tax_rate/notion_delivery_label のいずれかが空欄)
  //    Codex Phase E-15 R2 H-2: 配送方法 missing も対象に追加
  const filterSql = explicitCodes && explicitCodes.length > 0
    ? `WHERE no.rakuten_manage_number IN (${explicitCodes.map(() => '?').join(',')})`
    : `WHERE (no.yahoo_title IS NULL OR no.yahoo_title = ''
            OR no.yahoo_price IS NULL OR no.yahoo_price <= 0
            OR no.notion_tax_rate IS NULL OR no.notion_tax_rate = ''
            OR no.notion_delivery_label IS NULL OR no.notion_delivery_label = '')`;
  const rows = db.prepare(`
    SELECT no.notion_page_id, no.rakuten_manage_number AS manage_number,
           no.yahoo_title, no.yahoo_price, no.notion_tax_rate, no.notion_delivery_label
    FROM notion_overrides no
    ${filterSql}
    ORDER BY no.rakuten_manage_number
    LIMIT ${lim}
  `).all(...(explicitCodes || []));

  if (rows.length === 0) {
    return { totalScanned: 0, proposed: 0, skipped: 0, applied: 0, errors: 0, details: { proposed: [], skipped: [], applied: [], errors: [] } };
  }

  // 2. 楽天 RMS bulk fetch
  const manageNumbers = rows.map((r) => r.manage_number);
  let rakutenItems = [];
  let rakutenFailed = [];
  try {
    const r = await fetchItemDetailsBulkDetailed(manageNumbers);
    rakutenItems = r.items || [];
    rakutenFailed = r.failed || [];
  } catch (e) {
    const err = new Error(`rakuten_rms: ${e.message}`);
    err.statusCode = 502;
    err.cause = e;
    throw err;
  }
  const rakutenByMn = new Map(rakutenItems.map((it) => [it.manageNumber, it]));

  // 3. 各 SKU で proposal 構築 + (apply mode なら) PATCH
  const proposed = [];
  const skipped = [];
  const applied = [];
  const errors = [];
  for (const r of rows) {
    const rakutenItem = rakutenByMn.get(r.manage_number);
    if (!rakutenItem) {
      // Codex R2 軽微: fetchItemDetailsBulkDetailed の failed shape は { manageNumber, reason }
      const f = rakutenFailed.find((f) => f.manageNumber === r.manage_number);
      skipped.push({ itemCode: r.manage_number, reason: 'rakuten_fetch_failed', detail: f?.reason || null });
      continue;
    }
    const { proposed: prop, skipped: skip } = buildNotionDraftProposal(rakutenItem, r);
    if (Object.keys(prop).length === 0) {
      skipped.push({ itemCode: r.manage_number, reason: 'all_skipped', detail: skip });
      continue;
    }
    proposed.push({ itemCode: r.manage_number, manage_number: r.manage_number, proposed: prop, skipped: skip });

    if (!dryRun) {
      try {
        const properties = toNotionProperties(prop);
        await patchPageProperties(r.notion_page_id, properties);
        // 自 DB cache も即反映 (Notion 側の次回 sync を待たない)
        try {
          const updates = [];
          const params = [];
          if (prop.yahoo_title)      { updates.push('yahoo_title = ?');      params.push(prop.yahoo_title); }
          if (prop.yahoo_price != null) { updates.push('yahoo_price = ?');    params.push(prop.yahoo_price); }
          if (prop.notion_tax_rate)  { updates.push('notion_tax_rate = ?');   params.push(prop.notion_tax_rate); }
          if (prop.notion_delivery_label) { updates.push('notion_delivery_label = ?'); params.push(prop.notion_delivery_label); }
          if (updates.length > 0) {
            updates.push(`synced_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')`);
            params.push(r.notion_page_id);
            db.prepare(`UPDATE notion_overrides SET ${updates.join(', ')} WHERE notion_page_id = ?`).run(...params);
          }
        } catch (_) { /* best-effort */ }
        applied.push({ itemCode: r.manage_number, proposed: prop });
        if (audit) audit(db, 'notion_seed_draft', { itemCode: r.manage_number, proposed: prop });
      } catch (e) {
        errors.push({ itemCode: r.manage_number, error: e.message });
      }
    }
  }

  return {
    totalScanned: rows.length,
    proposed: proposed.length,
    skipped: skipped.length,
    applied: applied.length,
    errors: errors.length,
    details: { proposed, skipped, applied, errors },
  };
}
