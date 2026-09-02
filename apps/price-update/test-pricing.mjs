/**
 * test-pricing.mjs — 価格一括改定のガード判定 (要件 F3/F4) の検証
 *
 * ここが緩むと「桁を1つ間違えた値付け」がそのままモールに出る。M2 以降の実行側も同じ関数を通すので、
 * 画面を経由しない直叩きでも同じ判定になることを、この単体テストで担保する。
 *
 * 実行: node apps/price-update/test-pricing.mjs
 */
import { evaluateRow, estimateGross, costFloorCheck, isValidPrice, runLimits } from './pricing.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const has = (arr, needle, label) => ok(arr.some((s) => s.includes(needle)), `${label} — 実際: ${JSON.stringify(arr)}`);

const base = {
  mall: 'rakuten', confidence: 'confirmed', currentPrice: 1000, newPrice: 1200,
  cost: 500, taxRate: 0.10, shipping: 100, feeRate: 0.10,
};

console.log('\n── 正常系 ──');
{
  const r = evaluateRow(base);
  ok(r.canUpdate, '妥当な値上げは通る');
  ok(Math.abs(r.changeRatio - 0.2) < 1e-9, '変更率 +20%');
  ok(r.changeAmount === 200, '変更額 +200円');
  // 1200 - 550(原価税込) - 120(手数料) - 100(送料) = 430
  ok(r.estimate.gross === 430, `概算粗利 430 円 (実際 ${r.estimate.gross})`);
}

console.log('\n── 価格そのものの妥当性 ──');
{
  ok(!isValidPrice(0), '0円は不正');
  ok(!isValidPrice(-1), '負数は不正');
  ok(!isValidPrice(1.5), '小数は不正');
  ok(isValidPrice(1), '1円は有効');
  ok(!isValidPrice(1_000_000_000), '10億は上限超え');
  // 🚨M0実測: 楽天APIは0円を204で通してしまう。ここで止めないと本当に0円で公開される
  has(evaluateRow({ ...base, newPrice: 0 }).blocks, '0円は不可', '0円はブロック');
}

console.log('\n── 変更率ガード (決定事項#2: −30% 〜 +100%) ──');
{
  has(evaluateRow({ ...base, newPrice: 699 }).blocks, '値下げ幅', '−30.1% はブロック');
  ok(evaluateRow({ ...base, newPrice: 700 }).canUpdate, '−30.0% ちょうどは通る');
  ok(evaluateRow({ ...base, newPrice: 2000 }).canUpdate, '+100% ちょうどは通る');
  has(evaluateRow({ ...base, newPrice: 2001 }).blocks, '値上げ幅', '+100.1% はブロック');
  // 復旧 run だけは変更率を免除する (元の価格へ戻すため)
  // 元へ戻す値 (−34%) は通常ならブロックされるが、復旧 run では変更率だけ免除される
  ok(evaluateRow({ ...base, newPrice: 660, cost: 400, isRecovery: true }).canUpdate, '復旧 run は変更率を免除');
  has(evaluateRow({ ...base, newPrice: 660, cost: 400 }).blocks, '値下げ幅', '通常 run なら同じ値はブロック');
}

console.log('\n── 変更額ガード (+10万円) ──');
{
  const r = evaluateRow({ ...base, currentPrice: 200000, newPrice: 320000, cost: 100000 });
  has(r.blocks, '変更額が大きすぎます', '+12万円はブロック');
}

console.log('\n── 原価割れ (手数料に依存しない保守式) ──');
{
  // 原価550(税込) + 送料100 = 650 が下限
  has(evaluateRow({ ...base, newPrice: 649 }).blocks, '原価割れ', '649円はブロック');
  ok(evaluateRow({ ...base, currentPrice: 800, newPrice: 650 }).canUpdate, '650円ちょうどは通る');
  const noCost = evaluateRow({ ...base, cost: null });
  has(noCost.blocks, '原価が未登録', '原価未登録は判定不能 = ブロック (fail-closed)');
  ok(costFloorCheck({ price: 1000, cost: 500, taxRate: 0.08, shipping: 0 }).floor === 540, '軽減税率 8% も反映');
}

console.log('\n── 更新対象外のモール ──');
{
  has(evaluateRow({ ...base, mall: 'amazon' }).blocks, 'Amazon', 'Amazon は行データの側でも拒否 (決定事項⑥)');
  // ★au PAY は 2026-09-02 から更新できる (updateItemInfo が部分更新だと実測)。
  //   ここが「手動」に戻っていたら、画面で選べても送信の手前で必ず弾かれる
  ok(!evaluateRow({ ...base, mall: 'aupay' }).blocks.some((b) => /手動更新/.test(b)),
    'au PAY は更新できる (手動扱いに戻っていない)');
  has(evaluateRow({ ...base, mall: 'qoo10' }).blocks, '手動更新', 'Qoo10 はまだ手動');
}

console.log('\n── 引き当て・現在価格が確定していない行 ──');
{
  has(evaluateRow({ ...base, confidence: 'rule' }).blocks, '出品が確認できていない', '規則推定は更新不可');
  has(evaluateRow({ ...base, confidence: 'sales' }).blocks, '出品が確認できていない', '実績のみは更新不可');
  has(evaluateRow({ ...base, currentPrice: null }).blocks, '現在の設定価格を取得できていない', 'ライブ価格なしは更新不可');
  // 現在価格が 0 だと変更率が計算できず、−30%〜+100% の判定が丸ごと飛んでしまう
  const zeroNow = evaluateRow({ ...base, currentPrice: 0, newPrice: 99999 });
  has(zeroNow.blocks, '現在の設定価格が異常です', '現在価格 0円 は更新不可 (変更率ガードの迂回を塞ぐ)');
  ok(!zeroNow.canUpdate, '0円スタートで高額を入れても通らない');
  has(evaluateRow({ ...base, currentPrice: -100 }).blocks, '現在の設定価格が異常です', '負の現在価格も更新不可');
}

console.log('\n── 粗利警告 (ブロックはしない) ──');
{
  // 700 - 550 - 70 - 100 = -20 → 粗利率マイナス。ただし原価割れ (下限650) は超えているのでブロックはされない
  const r = evaluateRow({ ...base, newPrice: 700 });
  ok(r.canUpdate, '原価割れしていなければ更新はできる');
  has(r.warns, '粗利率が低いです', '粗利率が低ければ警告');
}

console.log('\n── 概算粗利の計算 ──');
{
  const e = estimateGross({ price: 1000, cost: 400, taxRate: 0.1, feeRate: 0.1, shipping: 50 });
  ok(e.fee === 100, '手数料 = 売価 × 近似率');
  ok(e.gross === 1000 - 440 - 100 - 50, '粗利 = 売価 − 原価(税込) − 手数料 − 配送費');
  ok(estimateGross({ price: 1000, cost: null, taxRate: 0.1, feeRate: 0.1, shipping: 0 }).gross === null,
    '原価が無ければ粗利は null (0円原価として計算しない)');
}

console.log('\n── run の上限 (env で下げられる) ──');
{
  const d = runLimits({});
  ok(d.maxNeCodes === 20 && d.maxSkuRows === 100, '既定は 20コード / 100行');
  const m2 = runLimits({ PRICE_UPDATE_MAX_NE_CODES: '5' });
  ok(m2.maxNeCodes === 5, 'env で 5 コードに下げられる (M2 初期)');
  ok(runLimits({ PRICE_UPDATE_MAX_NE_CODES: '0' }).maxNeCodes === 20, '不正な env は既定へ');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
