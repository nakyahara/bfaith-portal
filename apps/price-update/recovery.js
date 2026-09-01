/**
 * recovery.js — 復旧 run の組み立て (要件 F6・M2-4)
 *
 * 「さっき送った値上げを、元の価格に戻したい」を1手でやるためのもの。
 * 逆向きの run を人が手で作ると、戻す先の価格を打ち間違える余地が残る。
 * ここでは **戻す先を監査記録 (pu_operations.expected_current_price) から取る** ので、
 * 画面から価格を受け取らない = 打ち間違えようがない。
 *
 * ★戻す先は「元の run を作った時にモールから読んだ価格」。
 *   その後さらに誰かが手で変えていたら、送信時の楽観ロックが CONFLICT で止める
 *   (勝手に踏み潰さない)。
 *
 * 対象にする行 = **モールの価格が変わった可能性がある行**:
 *   confirmed … 変わったことを確認済み
 *   unknown   … 送ったが結果が分からない (変わっているかもしれない)
 *   failed    … 送った後の照合が通らなかった (変わっているかもしれない)
 * 対象にしない行:
 *   noop      … 既に同じ価格だった (戻す先も同じ値。送るだけ無駄)
 *   conflict / blocked / skipped … 送っていないので変わっていない
 *   previewed … まだ実行していない run (戻すものが無い)
 */

/** モールの価格が変わった可能性がある状態 */
export const RECOVERABLE_STATES = ['confirmed', 'unknown', 'failed'];

/**
 * 元の run から「戻す対象」を洗い出す。DB も fetch も触らない。
 * @param {object} sourceRun getRun() の戻り (operations に state が付いているもの)
 * @returns {{candidates: Array<object>, skipped: Array<{op:object, reason:string}>}}
 *   candidates の各要素 = { op, restoreTo }
 */
export function planRecovery(sourceRun) {
  const candidates = [];
  const skipped = [];
  for (const op of sourceRun.operations || []) {
    if (!RECOVERABLE_STATES.includes(op.state)) {
      skipped.push({ op, reason: `この行は送っていない、または価格が変わっていません (${op.state})` });
      continue;
    }
    const restoreTo = op.expected_current_price;
    if (restoreTo == null) {
      // 実行できた行には必ず入っている値。無い = 記録が壊れているので、黙って飛ばさず理由を残す
      skipped.push({ op, reason: '元の価格が記録に残っていないため戻せません' });
      continue;
    }
    if (op.new_price != null && op.new_price === restoreTo) {
      skipped.push({ op, reason: '送った価格と元の価格が同じです (戻す必要がありません)' });
      continue;
    }
    candidates.push({ op, restoreTo });
  }
  return { candidates, skipped };
}

/**
 * 復旧 run の operations を組み立てる。
 *
 * @param {Array<{op:object, restoreTo:number}>} candidates planRecovery の戻り
 * @param {Array<object>} previewRows いま引き当て直したプレビュー行 (ライブ価格つき)
 * @param {(row:object) => object} evaluate ガード評価 (isRecovery で呼ぶこと)
 * @returns {{operations: Array<object>, unmatched: Array<{op:object, reason:string}>}}
 */
export function buildRecoveryOperations(candidates, previewRows, evaluate) {
  const operations = [];
  const unmatched = [];
  const key = (mall, neCode, rowKind) => `${mall}|${String(neCode).toLowerCase().trim()}|${rowKind}`;
  const byKey = new Map();
  for (const r of previewRows) byKey.set(key(r.mall, r.neCode, r.rowKind), r);

  for (const { op, restoreTo } of candidates) {
    const row = byKey.get(key(op.mall, op.ne_code, op.row_kind));
    if (!row) {
      // 元の run にあった出品が、いま引き当て直すと出てこない (出品が消えた等)。人が見る
      unmatched.push({ op, reason: 'いま引き当て直すとこの出品が見つかりません。モールの画面で確認してください' });
      continue;
    }
    // ★戻す先はプレビューからではなく監査記録から取る (画面や再引き当ての値を信じない)
    const evaluated = evaluate({ ...row, newPrice: restoreTo });
    operations.push({
      mall: row.mall,
      neCode: row.neCode,
      rowKind: row.rowKind,
      viaCode: row.viaCode,
      productName: row.productName,
      listingCode: row.listingCode,
      skuCode: row.skuCode,
      confidence: row.confidence,
      priceSource: row.priceSource,
      priceFetchedAt: row.priceFetchedAt,
      // 楽観ロックの基準は「いまモールにある価格」。元の run の値ではない
      expectedCurrentPrice: row.price,
      newPrice: restoreTo,
      cost: row.cost,
      taxRate: row.taxRate,
      shipping: row.shipping,
      feeRate: row.feeRate,
      guard: evaluated.evaluation
        ? { blocks: evaluated.evaluation.blocks, warns: evaluated.evaluation.warns, canUpdate: evaluated.evaluation.canUpdate }
        : null,
      productUrl: row.url,
      initialState: evaluated.evaluation?.canUpdate ? 'previewed' : 'blocked_preview',
      // 監査用: どの行を戻そうとしているか
      sourceOperationId: op.operation_id,
    });
  }
  return { operations, unmatched };
}
