/**
 * test-rakuten-price-ops.mjs — 楽天 価格更新の判断と受領台帳の検証 (価格一括改定 M2)
 *
 * ここが緩むと「別のSKUに値付け」「二重更新」「知らないうちに新規SKUが生える」が起きる。
 * 楽天へは接続せず、GET 応答を模したオブジェクトに対して判断だけを試す。
 *
 * 実行: node apps/warehouse/test-rakuten-price-ops.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import {
  planPriceUpdate, toIntPrice, isValidPrice,
  createPriceOpsTables, receiveOperation, completeOperation, getOperation, replayOf,
} from './rakuten-price-ops.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

// 実物と同じ形: 価格は文字列で返る (M0実測)
const item = {
  manageNumber: '0726-001802',
  variants: {
    360: { standardPrice: '577', merchantDefinedSkuId: '0726-001802-BK' },
    361: { standardPrice: '577', merchantDefinedSkuId: '0726-001802-CL' },
  },
};

console.log('\n── 価格の読み取り ──');
{
  eq(toIntPrice('577'), 577, '文字列の価格を整数に');
  eq(toIntPrice('577.0'), null, '小数表記は読めない扱い');
  eq(toIntPrice(''), null, '空は null');
  ok(!isValidPrice(0), '0円は不正 (楽天APIは 204 で通してしまう)');
  ok(isValidPrice(1) && isValidPrice(999_999_999), '1円〜上限は有効');
  ok(!isValidPrice(1_000_000_000), '上限超えは不正');
}

console.log('\n── 正常系 ──');
{
  const plan = planPriceUpdate(item, { expected: { 360: 577 }, prices: { 360: 620 } });
  eq(plan.ok, true, '計画できる');
  eq(plan.patch, { variants: { 360: { standardPrice: 620 } } }, '★変更する SKU だけを送る');
  eq(plan.applied, { 360: 620 }, '適用内容を返す');

  const multi = planPriceUpdate(item, { expected: { 360: 577, 361: 577 }, prices: { 360: 620, 361: 630 } });
  eq(multi.patch.variants, { 360: { standardPrice: 620 }, 361: { standardPrice: 630 } }, '複数SKUは1回にまとめる');
}

console.log('\n── 楽観ロック (更新前価格が違えば書き換えない) ──');
{
  const conflict = planPriceUpdate(item, { expected: { 360: 500 }, prices: { 360: 620 } });
  eq([conflict.ok, conflict.code], [false, 'CONFLICT'], '★現在価格が想定と違えば CONFLICT');
  eq(conflict.detail.conflicts[0], { sku: '360', expected: 500, live: 577, reason: '現在価格が想定と違います' }, '食い違いの中身を返す');

  // 文字列の価格を整数化せずに比較すると全件 conflict になる (M0で踏んだ罠)
  const strExpected = planPriceUpdate(item, { expected: { 360: '577' }, prices: { 360: 620 } });
  eq(strExpected.ok, true, 'expected が文字列でも整数として照合する');

  const noExpected = planPriceUpdate(item, { expected: {}, prices: { 360: 620 } });
  eq([noExpected.ok, noExpected.code], [false, 'EXPECTED_REQUIRED'], '★expected 無しでは書き換えない');

  const unreadable = planPriceUpdate(
    { variants: { 360: { standardPrice: 'お問い合わせ' } } },
    { expected: { 360: 577 }, prices: { 360: 620 } });
  eq([unreadable.ok, unreadable.code], [false, 'CONFLICT'], '現在価格が読めない時も書き換えない');
}

console.log('\n── 存在しない SKU は送らない (新規SKU作成になるため) ──');
{
  const ghost = planPriceUpdate(item, { expected: { 999: 577 }, prices: { 999: 620 } });
  eq([ghost.ok, ghost.code], [false, 'SKU_NOT_FOUND'], '★M0実測: 存在しないキーは新規SKU作成と解釈される');
  ok(ghost.message.includes('新規SKU'), '理由に新規SKU作成の危険を書く');
  eq(ghost.detail.availableSkus, ['360', '361'], '実在するSKUを返す');

  const mixed = planPriceUpdate(item, { expected: { 360: 577, 999: 1 }, prices: { 360: 620, 999: 1 } });
  eq(mixed.ok, false, '1つでも存在しなければ全体を送らない (部分適用を作らない)');
}

console.log('\n── 価格そのものの検査 ──');
{
  eq(planPriceUpdate(item, { expected: { 360: 577 }, prices: { 360: 0 } }).code, 'INVALID_PRICE', '★0円は送らない');
  eq(planPriceUpdate(item, { expected: { 360: 577 }, prices: { 360: -1 } }).code, 'INVALID_PRICE', '負数は送らない');
  eq(planPriceUpdate(item, { expected: { 360: 577 }, prices: { 360: 620.5 } }).code, 'INVALID_PRICE', '小数は送らない');
  eq(planPriceUpdate(item, { expected: {}, prices: {} }).code, 'EMPTY_PRICES', '空は送らない');
  eq(planPriceUpdate({ variants: null }, { expected: {}, prices: { a: 1 } }).code, 'NO_VARIANTS', 'SKU情報が無ければ送らない');
}

console.log('\n── 同じ価格なら送らない ──');
{
  const same = planPriceUpdate(item, { expected: { 360: 577 }, prices: { 360: 577 } });
  eq([same.ok, same.noop], [true, true], '同価格は noop (楽天を無駄に叩かない)');
  const partial = planPriceUpdate(item, { expected: { 360: 577, 361: 577 }, prices: { 360: 577, 361: 630 } });
  eq(partial.patch.variants, { 361: { standardPrice: 630 } }, '変わる SKU だけ送る');
}

console.log('\n── 受領台帳 (冪等) ──');
{
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'price-ops-'));
  const db = createPriceOpsTables(new Database(path.join(dir, 'ops.db')));
  const req = { expected: { 360: 577 }, prices: { 360: 620 } };

  const first = receiveOperation(db, { operationId: 'op-0001-abcdef', runId: 'run-1', manageNumber: 'mn-1', request: req });
  eq(first.fresh, true, '初回は fresh');
  completeOperation(db, 'op-0001-abcdef', 'applied', { applied: { 360: 620 } });

  const again = receiveOperation(db, { operationId: 'op-0001-abcdef', runId: 'run-1', manageNumber: 'mn-1', request: req });
  eq(again.fresh, false, '★同じ operation_id は再実行しない');
  const replay = replayOf(again.row);
  eq([replay.replay, replay.state, replay.result], [true, 'applied', { applied: { 360: 620 } }], '前回結果をそのまま返す');

  // 受領はしたが結果が無い = 送信済みか不明。実行し直さない
  receiveOperation(db, { operationId: 'op-0002-abcdef', runId: 'run-1', manageNumber: 'mn-1', request: req });
  const unknown = replayOf(getOperation(db, 'op-0002-abcdef'));
  eq(unknown.state, 'unknown', '★結果が残っていない受領済みIDは unknown');
  ok(unknown.result.message.includes('実行しません'), '実行しないと明示する');

  // 結果は上書きしない (最初の結果が正)
  completeOperation(db, 'op-0001-abcdef', 'failed', { error: 'X' });
  eq(getOperation(db, 'op-0001-abcdef').result_state, 'applied', '★確定した結果は後から書き換わらない');

  db.close();
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* Windows のロック残りは無視 */ }
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
