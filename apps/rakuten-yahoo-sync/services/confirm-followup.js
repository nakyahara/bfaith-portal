/**
 * 確定値を保存した後に、その商品の出品前チェックを取り直す (2026-10-22)。
 *
 * 一覧の「修正必要」は jobs に保存した理由 (楽天の実データ検査の結果) も使うので、確定し直しても
 * 次の「全部更新」まで古い理由が残る。保存の応答は待たせず、裏で 1 商品だけ評価し直す
 * (楽天 RMS を読むので数秒〜数十秒。失敗しても次の全部更新で直る — best-effort)。
 *
 * confirmed-values.js からは呼ばない (publish-pipeline が confirmed-values を読むので循環する)。
 */
import { evaluateItemForPublish } from './publish-pipeline.js';

export function reevaluateAfterConfirm(db, manageNumber, { log = console } = {}) {
  const mn = String(manageNumber ?? '').trim().toLowerCase();
  if (!mn) return Promise.resolve(null);
  const rows = db.prepare(`
    SELECT item_code, rakuten_manage_number FROM migration_candidates
     WHERE lower(rakuten_manage_number) = ? OR lower(item_code) = ?
  `).all(mn, mn);
  return Promise.all(rows.map((c) =>
    evaluateItemForPublish({ db, itemCode: c.item_code, manageNumber: c.rakuten_manage_number || c.item_code, dryRun: false })
      .catch((e) => { log.warn(`[rys-confirm] 出品前チェックの取り直しに失敗 ${c.item_code}: ${e.message}`); return null; })));
}
