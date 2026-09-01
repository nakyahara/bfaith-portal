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
 *   unknown / failed … ただし **状態だけでは決められない**。
 *     failed には「送った後の照合が通らなかった」(変わったかもしれない) と
 *     「miniPC が送る前に弾いた 400 / 商品が無い」(変わっていない) が混ざっている。
 *     → 実行側が記録した `mayHaveChanged` の印で判断する (execute.js が付ける)。
 *       印が無い古い記録は、状態だけで判断せず **対象外** にする (勝手に戻さない方に倒す)
 * 対象にしない行:
 *   noop      … 既に同じ価格だった (戻す先も同じ値。送るだけ無駄)
 *   conflict / blocked / skipped … 送っていないので変わっていない
 *   previewed … まだ実行していない run (戻すものが無い)
 */

import { mergeGuardWarns } from './pricing.js';

/** 変わったことが確認できている状態 */
export const CHANGED_STATES = ['confirmed'];
/** 変わったかもしれない状態 (mayHaveChanged の印があるものだけ対象にする) */
export const MAYBE_CHANGED_STATES = ['unknown', 'failed'];
/** 復旧の候補になりうる状態 (画面表示用) */
export const RECOVERABLE_STATES = [...CHANGED_STATES, ...MAYBE_CHANGED_STATES];

/** その行の最後のイベントに mayHaveChanged の印があるか */
function mayHaveChanged(sourceRun, operationId) {
  let hit = null;
  for (const e of sourceRun.events || []) {
    if (e.operation_id !== operationId) continue;
    let detail = null;
    try { detail = e.detail_json ? JSON.parse(e.detail_json) : null; } catch { detail = null; }
    if (detail && Object.prototype.hasOwnProperty.call(detail, 'mayHaveChanged')) hit = detail.mayHaveChanged === true;
  }
  return hit === true;
}

/**
 * 元の run から「戻す対象」を洗い出す。DB も fetch も触らない。
 * @param {object} sourceRun getRun() の戻り (operations に state が付いているもの)
 * @returns {{candidates: Array<object>, skipped: Array<{op:object, reason:string, blocking:boolean}>}}
 *   candidates の各要素 = { op, restoreTo }
 *   skipped の blocking = true は **価格が変わったのに戻せない** 行 (人に知らせないといけない)。
 *   false は「そもそも戻す必要が無い」行 (送っていない・既に同じ価格)。
 */
export function planRecovery(sourceRun) {
  const candidates = [];
  const skipped = [];
  for (const op of sourceRun.operations || []) {
    if (!RECOVERABLE_STATES.includes(op.state)) {
      skipped.push({ op, blocking: false, reason: `この行は送っていない、または価格が変わっていません (${op.state})` });
      continue;
    }
    // ★「失敗」には送る前に弾かれたものも混ざる。印が無ければ戻さない (変わっていない行を上書きしない)
    if (MAYBE_CHANGED_STATES.includes(op.state) && !mayHaveChanged(sourceRun, op.operation_id)) {
      skipped.push({ op, blocking: false, reason: 'モールに送る前に止まった行です (価格は変わっていないので戻す必要がありません)' });
      continue;
    }
    const restoreTo = op.expected_current_price;
    if (restoreTo == null) {
      // ★価格は変わったのに戻す先が分からない。いちばん人に知らせないといけない行 (blocking)
      skipped.push({ op, blocking: true, reason: '元の価格が記録に残っていないため戻せません。モールの画面で実際の価格を確認してください' });
      continue;
    }
    if (op.new_price != null && op.new_price === restoreTo) {
      skipped.push({ op, blocking: false, reason: '送った価格と元の価格が同じです (戻す必要がありません)' });
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
  // ★突き合わせは **出品コードと SKU まで** 見る。モール × NEコード × 単品/セット だけだと、
  //   同じ NE コードに出品が 2 つあるモールで別の出品に値付けしうる (Codex R1 重大)
  const key = (mall, neCode, rowKind, listingCode, skuCode) => [
    mall, String(neCode ?? '').toLowerCase().trim(), rowKind,
    String(listingCode ?? '').toLowerCase().trim(), String(skuCode ?? '').toLowerCase().trim(),
  ].join('|');
  /**
   * ★旧形式 (色の個別商品コードを sku_code にしていた頃) の突き合わせキー。
   *   Yahoo は商品に1つの価格しか持たないので、同じ商品の色は全部この行が受け持つ。
   *   いま引き当て直した行にぶら下がっている色だけを対象にする
   *   (その色が今も同じ商品にあることを確かめてから読み替える)。
   */
  const legacyYahooKeys = (r) => {
    if (r.mall !== 'yahoo') return [];
    const olds = [...(r.sharedSubCodes || []), ...(r.matchedSubCode ? [r.matchedSubCode] : [])];
    return [...new Set(olds.map((c) => String(c ?? '').toLowerCase().trim()).filter(Boolean))]
      .filter((c) => c !== String(r.skuCode ?? '').toLowerCase().trim())
      .map((c) => key(r.mall, r.neCode, r.rowKind, r.listingCode, c));
  };
  const byKey = new Map();
  const dup = new Set();
  for (const r of previewRows) {
    const k = key(r.mall, r.neCode, r.rowKind, r.listingCode, r.skuCode);
    if (byKey.has(k)) dup.add(k);      // 同じキーが 2 行 = どちらか決められない
    byKey.set(k, r);
    // ★2026-09-01 より前の Yahoo の記録は sku_code に **色の個別商品コード** が入っている。
    //   いまは商品コードに変えたので、そのままだと突き合わせが外れて「戻せない」になる。
    //   同じ商品を指しているのが確かめられる時 (その色がいまも同じ商品にぶら下がっている) だけ、
    //   旧いキーからも同じ行を引けるようにする。曖昧なら足さない (取り違えない)
    for (const old of legacyYahooKeys(r)) {
      if (byKey.has(old) && byKey.get(old) !== r) dup.add(old);
      else if (!byKey.has(old)) byKey.set(old, r);
    }
  }

  for (const { op, restoreTo } of candidates) {
    const k = key(op.mall, op.ne_code, op.row_kind, op.listing_code, op.sku_code);
    if (dup.has(k)) {
      unmatched.push({ op, reason: '同じ出品コード・SKU の行が複数あります。取り違えを避けるため戻しません' });
      continue;
    }
    const row = byKey.get(k);
    if (!row) {
      // 元の run にあった出品が、いま引き当て直すと出てこない (出品が消えた・別の出品コードになった等)。人が見る
      unmatched.push({
        op,
        reason: `いま引き当て直すと同じ出品 (${op.listing_code || '出品コード不明'}`
          + `${op.sku_code ? ' / SKU ' + op.sku_code : ''}) が見つかりません。モールの画面で確認してください`,
      });
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
      // ★注意書き (例: Yahoo は色ごとの価格を持たないので全色が変わる) は復旧でも同じ。
      //   価格を書き換える操作である以上、通常 run と同じものを見せる
      guard: mergeGuardWarns(evaluated.evaluation, row.sharedNote),
      productUrl: row.url,
      initialState: evaluated.evaluation?.canUpdate ? 'previewed' : 'blocked_preview',
      // 監査用: どの行を戻そうとしているか
      sourceOperationId: op.operation_id,
    });
  }
  return { operations, unmatched };
}
