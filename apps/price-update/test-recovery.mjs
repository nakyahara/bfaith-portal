/**
 * test-recovery.mjs — 復旧 run の組み立ての検証 (要件 F6・M2-4)
 *
 * ここが緩むと「戻したつもりで別の値を送る」「変わっていない行まで送る」が起きる。
 * DB も モールAPI も触らず、組み立てだけを試す。
 *
 * 実行: node apps/price-update/test-recovery.mjs
 */
import { planRecovery, buildRecoveryOperations, RECOVERABLE_STATES } from './recovery.js';
import { evaluateRow } from './pricing.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const op = (id, state, { expected = 577, next = 578, mall = 'rakuten', neCode = 'abc-001', rowKind = 'single' } = {}) => ({
  operation_id: id, state, mall, ne_code: neCode, row_kind: rowKind,
  expected_current_price: expected, new_price: next,
});

console.log('\n── 戻す対象になる行 ──');
{
  const run = { operations: [
    op('a', 'confirmed'),
    op('b', 'unknown'),
    op('c', 'failed'),
    op('d', 'noop'),
    op('e', 'conflict'),
    op('f', 'skipped'),
    op('g', 'blocked'),
    op('h', 'previewed'),
  ] };
  const { candidates, skipped } = planRecovery(run);
  eq(candidates.map((c) => c.op.operation_id), ['a', 'b', 'c'],
    '★価格が変わった可能性のある行だけ (確認ずみ / 結果不明 / 送った後の照合が通らなかった)');
  eq(RECOVERABLE_STATES, ['confirmed', 'unknown', 'failed'], '対象の状態は3つ');
  eq(skipped.length, 5, '残りは理由つきで対象外');
  ok(skipped.every((s) => s.reason), 'なぜ対象外かを必ず残す');
  eq(candidates[0].restoreTo, 577, '★戻す先は「元の run が記録した送信前の価格」');
}

console.log('\n── 戻す先が無い / 戻す必要が無い行 ──');
{
  const run = { operations: [
    { ...op('a', 'confirmed'), expected_current_price: null },
    { ...op('b', 'confirmed'), expected_current_price: 577, new_price: 577 },
  ] };
  const { candidates, skipped } = planRecovery(run);
  eq(candidates.length, 0, '両方とも対象外');
  ok(/元の価格が記録に残っていない/.test(skipped[0].reason), '記録が壊れている行は黙って飛ばさない: ' + skipped[0].reason);
  ok(/戻す必要がありません/.test(skipped[1].reason), '送った値と元の値が同じなら戻す必要が無い');
}

console.log('\n── operations の組み立て ──');
{
  const previewRow = {
    mall: 'rakuten', neCode: 'abc-001', rowKind: 'single', viaCode: null,
    productName: 'テスト商品', listingCode: 'mn-1', skuCode: 'sku-a', confidence: 'confirmed',
    priceSource: '楽天RMS (ライブ)', priceFetchedAt: '2026-09-01T00:00:00Z',
    price: 578,                       // ★いまモールにある価格 (さっき上げた値)
    cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12, url: 'https://example.com',
  };
  const evaluate = (row) => ({ evaluation: evaluateRow({ ...row, currentPrice: row.price, isRecovery: true }) });
  const { candidates } = planRecovery({ operations: [op('a', 'confirmed')] });
  const { operations, unmatched } = buildRecoveryOperations(candidates, [previewRow], evaluate);

  eq(unmatched.length, 0, '引き当て直せた');
  eq(operations.length, 1, '1 行');
  const o = operations[0];
  eq(o.newPrice, 577, '★新売価 = 監査記録の「送信前の価格」(画面から受け取らない)');
  eq(o.expectedCurrentPrice, 578, '★楽観ロックの基準 = いまモールにある価格 (元の run の値ではない)');
  eq(o.initialState, 'previewed', 'ガードを通ったので実行候補');
  eq(o.sourceOperationId, 'a', '元の行を記録に残す');
  eq(o.listingCode, 'mn-1', '出品コードは引き当て直した値');
}

console.log('\n── 引き当て直すと出てこない行は、黙って飛ばさない ──');
{
  const { candidates } = planRecovery({ operations: [op('a', 'confirmed')] });
  const { operations, unmatched } = buildRecoveryOperations(candidates, [], () => ({ evaluation: null }));
  eq(operations.length, 0, '組み立てない');
  eq(unmatched.length, 1, '未一致として返す');
  ok(/見つかりません/.test(unmatched[0].reason), '理由を残す: ' + unmatched[0].reason);
}

console.log('\n── 別の商品の行に取り違えない ──');
{
  const rows = [
    { mall: 'rakuten', neCode: 'abc-001', rowKind: 'single', price: 578, confidence: 'confirmed', cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12 },
    { mall: 'rakuten', neCode: 'abc-002', rowKind: 'single', price: 900, confidence: 'confirmed', cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12 },
    { mall: 'yahoo', neCode: 'abc-001', rowKind: 'single', price: 700, confidence: 'confirmed', cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12 },
    { mall: 'rakuten', neCode: 'abc-001', rowKind: 'set', price: 1500, confidence: 'confirmed', cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12 },
  ];
  const evaluate = (row) => ({ evaluation: evaluateRow({ ...row, currentPrice: row.price, isRecovery: true }) });
  const { candidates } = planRecovery({ operations: [op('a', 'confirmed')] });
  const { operations } = buildRecoveryOperations(candidates, rows, evaluate);
  eq([operations.length, operations[0].mall, operations[0].neCode, operations[0].rowKind, operations[0].expectedCurrentPrice],
    [1, 'rakuten', 'abc-001', 'single', 578], '★モール × NEコード × 単品/セット が全部一致した行だけ');
  // NEコードの大小文字・前後空白が違っても同じ行として扱う
  const { operations: o2 } = buildRecoveryOperations(
    planRecovery({ operations: [op('a', 'confirmed', { neCode: ' ABC-001 ' })] }).candidates, rows, evaluate);
  eq(o2.length, 1, '大小文字・前後空白は無視して一致させる');
}

console.log('\n── ガード: 復旧では変更率だけ免除される ──');
{
  // 900円 → 400円 に戻す = −55.6%。通常の run なら「値下げ幅が大きすぎます」で止まる
  const down = { currentPrice: 900, newPrice: 400, cost: 210, taxRate: 0.1, shipping: 100, feeRate: 0.12, mall: 'rakuten', confidence: 'confirmed' };
  ok(evaluateRow(down).blocks.some((b) => /値下げ幅/.test(b)), '通常の run なら大きな値下げは止まる');
  ok(!evaluateRow({ ...down, isRecovery: true }).blocks.some((b) => /値下げ幅/.test(b)), '★復旧では変更率を見ない');

  // ★免除されるのは変更率だけ。0円・原価割れ・出品未確定は復旧でも止める
  eq(evaluateRow({ ...down, isRecovery: true, newPrice: 0 }).canUpdate, false, '0円は復旧でも止める');
  ok(evaluateRow({ ...down, isRecovery: true, newPrice: 200 }).blocks.some((b) => /原価割れ/.test(b)), '原価割れは復旧でも止める');
  eq(evaluateRow({ ...down, isRecovery: true, confidence: 'rule' }).canUpdate, false, '出品が確定していなければ復旧でも止める');
  eq(evaluateRow({ ...down, isRecovery: true, currentPrice: null }).canUpdate, false, '現在価格が取れなければ復旧でも止める');
}

console.log('\n── ガードに掛かった行は実行候補にしない ──');
{
  const previewRow = {
    mall: 'rakuten', neCode: 'abc-001', rowKind: 'single', confidence: 'confirmed',
    price: 578, cost: 100000, taxRate: 0.1, shipping: 182, feeRate: 0.12,   // 原価が高すぎて戻すと原価割れ
  };
  const evaluate = (row) => ({ evaluation: evaluateRow({ ...row, currentPrice: row.price, isRecovery: true }) });
  const { candidates } = planRecovery({ operations: [op('a', 'confirmed')] });
  const { operations } = buildRecoveryOperations(candidates, [previewRow], evaluate);
  eq(operations[0].initialState, 'blocked_preview', '★ガードに掛かったら blocked_preview (実行候補に混ぜない)');
  ok(operations[0].guard.blocks.length > 0, '止めた理由を記録に残す');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
