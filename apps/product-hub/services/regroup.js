/**
 * 子SKUで作られたドラフトを NE の代表商品コードにまとめる。
 *
 * ne_code は Notion カード・NE 突合のキーなので、付け替えは fail-closed で狭く許可する:
 *   - Notion カードの作成は「旧 ne_code を読む → 外部API → notion_page_id 確定」の順。
 *     途中で ne_code が変わると旧コードのカードが新コードに紐づくため、
 *     **一度も作成を試みていない (pending / page_id なし / attempts=0)** ものだけ許可する
 *   - 取り込み由来 (Notion が正) は不可
 *   - 画面に出した from/to と、実行時に再計算した値が一致した場合だけ適用する (TOCTOU)
 *
 * 判定・照合・更新・監査ログを **すべて 1 トランザクション内**で行う (Codex R2 high-2)。
 * 事前チェックとUPDATEの間に別プロセスが状態を変えても、WHERE 側の CAS で弾く。
 */
import { logEvent } from '../db.js';
import { resolveVariationGroup } from '../lib/variation.js';

/**
 * @returns {{ code: number, error?: string, ne_code?: string, from?: string }}
 *   code=200 成功 / 400 対象外・禁止 / 404 消えた / 409 競合・表示が古い
 */
export function regroupToRepCode(db, draftId, { expectedFrom, expectedTo, actor = null } = {}) {
  if (!expectedFrom || !expectedTo) {
    return { code: 400, error: 'expected_from / expected_to が必要です' };
  }

  const run = db.transaction(() => {
    const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
    if (!draft) return { code: 404, error: 'draft not found' };

    // mirror も同じ接続で読むので、トランザクション内では一貫したスナップショットを見る
    const v = resolveVariationGroup(db, draft.ne_code, { withMembers: false });
    if (v.kind !== 'variation' || !v.isChild) {
      return { code: 400, error: 'この商品はまとめる対象ではありません' };
    }
    const blocked = regroupBlockReason(db, draft, v);
    if (blocked) return { code: 400, error: blocked };

    if (expectedFrom !== draft.ne_code || expectedTo !== v.groupKey) {
      return {
        code: 409,
        error: `画面の表示内容が古くなっています (現在: ${draft.ne_code} → ${v.groupKey})。再読み込みして確認しなおしてください`,
      };
    }

    // CAS: 事前チェックで見た前提をすべて WHERE に載せる (Codex R2 high-1)
    const info = db.prepare(`
      UPDATE product_drafts
      SET ne_code = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE id = ?
        AND ne_code = ?
        AND source = 'portal'
        AND notion_page_id IS NULL
        AND notion_card_status = 'pending'
        AND notion_card_attempts = 0
    `).run(v.groupKey, draftId, draft.ne_code);
    if (info.changes !== 1) {
      return { code: 409, error: '他の操作と競合しました。画面を再読み込みしてください' };
    }
    logEvent(db, draftId, 'regrouped_to_rep_code', `${draft.ne_code} → ${v.groupKey}`, actor);
    return { code: 200, ne_code: v.groupKey, from: draft.ne_code };
  });

  try {
    return run();
  } catch (e) {
    if (/UNIQUE/i.test(String(e && e.message))) {
      return { code: 409, error: `代表商品コード ${expectedTo} のドラフトは既にあります` };
    }
    throw e;
  }
}

/**
 * 「代表商品コードにまとめる」を出してよいか。出せない理由 (文字列) か null を返す。
 * 画面表示 (ボタンを出すか) と実行時チェックの両方から呼ぶ。
 */
export function regroupBlockReason(db, draft, variation) {
  if (!variation || variation.kind !== 'variation' || !variation.isChild) return null;
  if (draft.source !== 'portal') {
    return 'Notionから取り込んだ商品はNotion側が正のため、ここでは商品コードを変更できません';
  }
  if (draft.notion_page_id || draft.notion_card_status !== 'pending' || draft.notion_card_attempts > 0) {
    return 'Notionカードの作成が進行中/完了済みのため変更できません (先にNotion側の商品コードを直してください)';
  }
  const conflict = db.prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?')
    .get(String(variation.groupKey).trim().toLowerCase());
  if (conflict) {
    return `代表商品コード ${variation.groupKey} のドラフトは既にあります (#${conflict.id})`;
  }
  return null;
}
