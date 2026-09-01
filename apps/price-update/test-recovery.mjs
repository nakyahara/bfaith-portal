/**
 * test-recovery.mjs — 復旧 run の組み立ての検証 (要件 F6・M2-4)
 *
 * ここが緩むと「戻したつもりで別の値を送る」「変わっていない行まで送る」が起きる。
 * DB も モールAPI も触らず、組み立てだけを試す。
 *
 * 実行: node apps/price-update/test-recovery.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { planRecovery, buildRecoveryOperations, RECOVERABLE_STATES } from './recovery.js';
import { evaluateRow } from './pricing.js';
import { createTables, insertRun, getRun, claimRun, createRecoveryRun, recoveryRunsOf } from './db.js';
import { executeRun } from './execute.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const op = (id, state, { expected = 577, next = 578, mall = 'rakuten', neCode = 'abc-001', rowKind = 'single',
  listingCode = 'mn-1', skuCode = 'sku-a' } = {}) => ({
  operation_id: id, state, mall, ne_code: neCode, row_kind: rowKind,
  listing_code: listingCode, sku_code: skuCode,
  expected_current_price: expected, new_price: next,
});
/** 実行側が付ける「価格が変わったかもしれない」印 */
const mark = (id, may) => ({ operation_id: id, detail_json: JSON.stringify({ mayHaveChanged: may }) });

console.log('\n── 戻す対象になる行 ──');
{
  const run = { operations: [
    op('a', 'confirmed'),
    op('b', 'unknown'),
    op('c', 'failed'),
    op('c2', 'failed'),     // 送る前に弾かれた失敗 (印が false)
    op('c3', 'failed'),     // 印そのものが無い古い記録
    op('d', 'noop'),
    op('e', 'conflict'),
    op('f', 'skipped'),
    op('g', 'blocked'),
    op('h', 'previewed'),
  ], events: [mark('b', true), mark('c', true), mark('c2', false)] };
  const { candidates, skipped } = planRecovery(run);
  eq(candidates.map((c) => c.op.operation_id), ['a', 'b', 'c'],
    '★価格が変わった可能性のある行だけ (確認ずみ / 印つきの 結果不明・失敗)');
  eq(RECOVERABLE_STATES, ['confirmed', 'unknown', 'failed'], '候補になりうる状態は3つ');
  eq(skipped.length, 7, '残りは理由つきで対象外');
  ok(skipped.every((s) => s.reason), 'なぜ対象外かを必ず残す');
  eq(candidates[0].restoreTo, 577, '★戻す先は「元の run が記録した送信前の価格」');

  // ★「失敗」には送る前に弾かれたもの (400 / 商品が無い) が混ざる。印が無ければ戻さない
  const c2 = skipped.find((s) => s.op.operation_id === 'c2');
  const c3 = skipped.find((s) => s.op.operation_id === 'c3');
  ok(/送る前に止まった/.test(c2.reason), '★送信前に弾かれた失敗は戻さない: ' + c2.reason);
  ok(/送る前に止まった/.test(c3.reason), '★印が無い古い記録も戻さない (勝手に戻さない方に倒す)');
  ok(skipped.every((x) => x.blocking === false), '送っていない行は blocking にしない (騒がない)');
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
  eq([skipped[0].blocking, skipped[1].blocking], [true, false],
    '★「変わったのに戻せない」行だけ blocking (人に知らせる) / 「戻す必要が無い」行は false');
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
  const base = { confidence: 'confirmed', cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12 };
  const rows = [
    { ...base, mall: 'rakuten', neCode: 'abc-001', rowKind: 'single', listingCode: 'mn-1', skuCode: 'sku-a', price: 578 },
    { ...base, mall: 'rakuten', neCode: 'abc-002', rowKind: 'single', listingCode: 'mn-2', skuCode: 'sku-a', price: 900 },
    { ...base, mall: 'yahoo', neCode: 'abc-001', rowKind: 'single', listingCode: 'mn-1', skuCode: 'sku-a', price: 700 },
    { ...base, mall: 'rakuten', neCode: 'abc-001', rowKind: 'set', listingCode: 'mn-1', skuCode: 'sku-a', price: 1500 },
    // ★同じ NE コード・同じモール・同じ単品だが **別の出品** (楽天に2出品ある商品)
    { ...base, mall: 'rakuten', neCode: 'abc-001', rowKind: 'single', listingCode: 'mn-OTHER', skuCode: 'sku-z', price: 3000 },
  ];
  const evaluate = (row) => ({ evaluation: evaluateRow({ ...row, currentPrice: row.price, isRecovery: true }) });
  const { candidates } = planRecovery({ operations: [op('a', 'confirmed')] });
  const { operations } = buildRecoveryOperations(candidates, rows, evaluate);
  eq([operations.length, operations[0].listingCode, operations[0].skuCode, operations[0].expectedCurrentPrice],
    [1, 'mn-1', 'sku-a', 578], '★モール × NEコード × 単品/セット × 出品コード × SKU が全部一致した行だけ');

  // NEコード・出品コード・SKU の大小文字・前後空白が違っても同じ行として扱う
  const { operations: o2 } = buildRecoveryOperations(
    planRecovery({ operations: [op('a', 'confirmed', { neCode: ' ABC-001 ', listingCode: 'MN-1', skuCode: ' SKU-A ' })] }).candidates,
    rows, evaluate);
  eq(o2.length, 1, '大小文字・前後空白は無視して一致させる');

  // ★元の run で更新したのとは違う出品しか今は無い → 戻さない (別出品に値付けしない)
  const { operations: o3, unmatched: u3 } = buildRecoveryOperations(
    planRecovery({ operations: [op('a', 'confirmed', { listingCode: 'mn-GONE' })] }).candidates, rows, evaluate);
  eq([o3.length, u3.length], [0, 1], '★出品コードが変わっていたら戻さない');
  ok(/mn-GONE/.test(u3[0].reason), '理由に元の出品コードを書く: ' + u3[0].reason);

  // ★同じキーの行が2つ返ってきたらどちらか決めない
  const { operations: o4, unmatched: u4 } = buildRecoveryOperations(
    planRecovery({ operations: [op('a', 'confirmed')] }).candidates, [rows[0], { ...rows[0], price: 999 }], evaluate);
  eq([o4.length, u4.length], [0, 1], '★同じ出品コード・SKU の行が複数なら戻さない');
  ok(/複数あります/.test(u4[0].reason), '理由: ' + u4[0].reason);
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
    listingCode: 'mn-1', skuCode: 'sku-a',
    price: 578, cost: 100000, taxRate: 0.1, shipping: 182, feeRate: 0.12,   // 原価が高すぎて戻すと原価割れ
  };
  const evaluate = (row) => ({ evaluation: evaluateRow({ ...row, currentPrice: row.price, isRecovery: true }) });
  const { candidates } = planRecovery({ operations: [op('a', 'confirmed')] });
  const { operations } = buildRecoveryOperations(candidates, [previewRow], evaluate);
  eq(operations[0].initialState, 'blocked_preview', '★ガードに掛かったら blocked_preview (実行候補に混ぜない)');
  ok(operations[0].guard.blocks.length > 0, '止めた理由を記録に残す');
}

console.log('\n── 実行の直前でも復旧として評価する (作成時だけ免除しても意味がない) ──');
{
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'pu-rec-'));
  const db = createTables(new Database(path.join(dir, 'm.db')));
  // 900 円 → 400 円に戻す (−55.6%)。通常の run なら送信直前のガードで止まる値
  const mkRun = (kind) => {
    const runId = insertRun(db, {
      createdBy: 't@example.com', kind, neCodes: ['abc-001'], limits: {},
      operations: [{
        operationId: 'puo-' + kind, mall: 'rakuten', neCode: 'abc-001', rowKind: 'single',
        listingCode: 'mn-1', skuCode: 'sku-a', confidence: 'confirmed',
        expectedCurrentPrice: 900, newPrice: 400,
        cost: 210, taxRate: 0.1, shipping: 100, feeRate: 0.12, initialState: 'previewed',
      }],
    });
    return getRun(db, runId);
  };
  const client = () => ({
    calls: [],
    patchItemPrices: async function (mn, body) { this.calls.push({ mn, body }); return { status: 200, body: { ok: true, state: 'applied' } }; },
    fetchItemDetail: async () => ({ item: { manageNumber: 'mn-1', variants: { 'sku-a': { standardPrice: '400' } } }, status: 'found' }),
  });
  const ENV_ON = { PRICE_UPDATE_RAKUTEN_ENABLED: '1' };

  const c1 = client();
  const normal = await executeRun(db, mkRun('normal'), { actor: 't', client: c1, env: ENV_ON });
  eq([c1.calls.length, normal.summary.skipped], [0, 1], '通常の run は大きな値下げを送信直前で止める');

  const c2 = client();
  const rec = await executeRun(db, mkRun('recovery'), { actor: 't', client: c2, env: ENV_ON });
  eq([c2.calls.length, rec.summary.applied], [1, 1], '★復旧 run は送信直前のガードでも変更率を免除される');

  db.close();
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* Windows のロック残り */ }
}

console.log('\n── 復旧 run を二重に作らせない ──');
{
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'pu-rec2-'));
  const db = createTables(new Database(path.join(dir, 'm.db')));
  const srcId = insertRun(db, {
    createdBy: 't@example.com', neCodes: ['abc-001'], limits: {},
    operations: [{
      operationId: 'puo-src', mall: 'rakuten', neCode: 'abc-001', rowKind: 'single',
      confidence: 'confirmed', expectedCurrentPrice: 577, newPrice: 578, initialState: 'previewed',
    }],
  });
  const spec = { createdBy: 't@example.com', neCodes: ['abc-001'], limits: {}, operations: [{
    operationId: 'puo-r1', mall: 'rakuten', neCode: 'abc-001', rowKind: 'single',
    confidence: 'confirmed', expectedCurrentPrice: 578, newPrice: 577, initialState: 'previewed',
  }] };

  const first = createRecoveryRun(db, { sourceRunId: srcId, runSpec: spec });
  eq(first.ok, true, '1本目は作れる');
  const second = createRecoveryRun(db, { sourceRunId: srcId, runSpec: spec });
  eq([second.ok, second.code, second.runId], [false, 'RECOVERY_EXISTS', first.runId],
    '★未実行の復旧 run があるうちは2本目を作らせない (そちらへ誘導)');
  eq(recoveryRunsOf(db, srcId).length, 1, '復旧 run は1本のまま');

  // 実行済みになったら、明示確認つきでだけもう一度作れる
  claimRun(db, first.runId, 't@example.com');
  const third = createRecoveryRun(db, { sourceRunId: srcId, runSpec: spec });
  eq([third.ok, third.code], [false, 'ALREADY_RECOVERED'], '★一度戻した履歴は、確認なしでは戻せない');
  const fourth = createRecoveryRun(db, { sourceRunId: srcId, runSpec: spec, allowRepeat: true });
  eq(fourth.ok, true, '明示確認があれば作れる');
  eq(recoveryRunsOf(db, srcId).length, 2, '2本目ができた');

  db.close();
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* Windows のロック残り */ }
}

console.log('\n── Yahoo カラバリ: 旧形式の記録を戻せる / 注意書きが消えない ──');
{
  // 2026-09-01 より前の Yahoo の記録は sku_code に **色の個別商品コード** が入っていた。
  // いまは商品コードに変えたので、そのままだと突き合わせが外れて「戻せない」になる。
  // その色が今も同じ商品にぶら下がっていることを確かめたうえで読み替える
  const yahooRow = {
    mall: 'yahoo', neCode: '0726-001802-bk', rowKind: 'single', viaCode: null,
    productName: '合皮補修シート', listingCode: '0726-001802',
    skuCode: '0726-001802',                         // ★いまは商品コードが送り先
    matchedSubCode: '0726-001802-BK',
    sharedSubCodes: ['0726-001802-BK', '0726-001802-CL'],
    sharedNote: 'Yahoo は色ごとの価格を持ちません。変えるとこの商品の 2 色すべてが同じ価格になります',
    confidence: 'confirmed', priceSource: 'Yahoo itemInfo (ライブ)', priceFetchedAt: null,
    price: 720,
    cost: 210, taxRate: 0.1, shipping: 182, feeRate: 0.12, url: 'https://example.com',
  };
  const evaluate = (row) => ({ evaluation: evaluateRow({ ...row, currentPrice: row.price, isRecovery: true }) });

  // 旧形式で記録された行 (sku_code = 色の個別商品コード)
  const legacy = op('y1', 'confirmed', { mall: 'yahoo', neCode: '0726-001802-bk',
    listingCode: '0726-001802', skuCode: '0726-001802-BK', expected: 698, next: 720 });
  const r1 = buildRecoveryOperations(planRecovery({ operations: [legacy] }).candidates, [yahooRow], evaluate);
  eq(r1.unmatched.length, 0, '★旧形式 (色のコード) の記録でも戻す先を見つけられる');
  eq(r1.operations[0].skuCode, '0726-001802', '★送り先は商品コードに読み替わる');
  eq(r1.operations[0].newPrice, 698, '戻す先は監査記録の値');
  ok((r1.operations[0].guard.warns || []).some((w) => /色すべてが同じ価格/.test(w)),
    '★復旧 run でも「全色が変わる」注意書きが残る');

  // 新形式 (sku_code = 商品コード) はそのまま当たる
  const modern = op('y2', 'confirmed', { mall: 'yahoo', neCode: '0726-001802-bk',
    listingCode: '0726-001802', skuCode: '0726-001802', expected: 698, next: 720 });
  const r2 = buildRecoveryOperations(planRecovery({ operations: [modern] }).candidates, [yahooRow], evaluate);
  eq(r2.unmatched.length, 0, '新形式もそのまま戻せる');

  // その商品にぶら下がっていない色は読み替えない (別商品を掴まない)
  const alien = op('y3', 'confirmed', { mall: 'yahoo', neCode: '0726-001802-bk',
    listingCode: '0726-001802', skuCode: '0726-001802-ZZ', expected: 698, next: 720 });
  const r3 = buildRecoveryOperations(planRecovery({ operations: [alien] }).candidates, [yahooRow], evaluate);
  eq(r3.unmatched.length, 1, '★いま存在しない色は読み替えない (取り違えない)');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
