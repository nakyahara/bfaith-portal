/**
 * test-engine.mjs — 判定エンジン (engine.js) の検証。
 *
 * ここが緩むと、将来の実行段階で「原価割れの値下げ」「桁違いの値付け」がそのまま通る。
 * 2026-03 の事故で起きたことを、そのまま落ちるケースとして固定してある (★印)。
 *
 * 実行: node apps/amazon-pricing/test-engine.mjs
 */
import {
  evaluateListing, computeCosts, grossAt, isValidPrice, describeInputs, ceilDivide,
  RULE_VERSION, MODES, REASONS, FLAGS, ACTIONS, FALLBACK_REFERRAL_RATE,
  customNeedsOffers, customTypeSummary, normalizeCustomType,
} from './engine.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(a === b, `${label} — 実際: ${JSON.stringify(a)} / 期待: ${JSON.stringify(b)}`);

// FBA・原価 1,000 円 (税込)・FBA手数料 400・販売手数料 10%・最低粗利 10% → 下限 = ceil(1400 / 0.8) = 1750
const base = {
  mode: 'buybox', my_price: 2000, buybox_price: 1900, buybox_is_mine: 0,
  floor_price: null, ceiling_price: null, offset_jpy: 0, min_margin_rate: null,
  channel: 'FBA', cost_incl_tax: 1000, cost_missing_parts: 0,
  referral_fee_rate: 0.10, fba_fee: 400, per_item_fee: 0, variable_closing_fee: 0, ship_cost: null, units_30d: 5,
};

console.log('\n── 下限の計算 (computeCosts) ──');
{
  const c = computeCosts(base);
  eq(c.floorPrice, 1750, '下限 = ceil((原価+FBA手数料)/(1-手数料率-最低粗利率))');
  ok(c.costKnown && !c.feeRateAssumed, '原価・手数料率が分かっている');
  eq(computeCosts({ ...base, cost_incl_tax: null }).floorPrice, null, '原価が無ければ下限は null (0 円として計算しない)');
  eq(computeCosts({ ...base, cost_missing_parts: 1 }).floorPrice, null, '構成品に原価の無いものがあれば下限は null');
  eq(computeCosts({ ...base, fba_fee: null }).floorPrice, null, 'FBA なのに FBA 手数料が無ければ下限は null');
  ok(computeCosts({ ...base, fba_fee: null }).flags.includes('FBA_FEE_UNKNOWN'), '  → 旗 FBA_FEE_UNKNOWN');
  eq(computeCosts({ ...base, channel: 'FBM', ship_cost: null }).floorPrice, null, '自己発送で送料が無ければ下限は null');
  eq(computeCosts({ ...base, channel: 'FBM', ship_cost: 300 }).floorPrice, Math.ceil(1300 / 0.8), '自己発送は FBA 手数料の代わりに送料');
  const assumed = computeCosts({ ...base, referral_fee_rate: null });
  ok(assumed.feeRateAssumed && assumed.feeRate === FALLBACK_REFERRAL_RATE && assumed.flags.includes('FEE_RATE_ASSUMED'), '手数料率が無ければ表示用に 15% と仮定して旗を立てる');
  ok(assumed.floorPrice === null && assumed.feeRateValid === false, '  ★ただし下限は出さない (Codex R5: 15% は安全側とは限らない)');
  eq(evaluateListing({ ...base, referral_fee_rate: null, buybox_price: 1800 }).action, 'hold', '  手数料率なし + カート 1800 → 保留 (以前は 1867 への値下げ)');
  eq(evaluateListing({ ...base, referral_fee_rate: null, buybox_price: 1800, floor_price: 1700 }).action, 'hold', '  人のストッパーがあっても保留');
  eq(evaluateListing({ ...base, referral_fee_rate: null, buybox_price: 2300 }).action, 'raise', '  値上げは出す');
  eq(computeCosts({ ...base, referral_fee_rate: 0.45, min_margin_rate: 0.6 }).floorPrice, null, '手数料率+最低粗利率が 100% 以上なら下限は出せない');
  eq(computeCosts({ ...base, min_margin_rate: 0.95 }).minMarginRate, 0.10, '最低粗利率 95% (範囲外) は既定 10% に戻す');
  eq(computeCosts({ ...base, min_margin_rate: 0.2 }).floorPrice, 2000, '最低粗利率 20% なら下限が上がる (1400/0.7 = 2000。浮動小数の誤差で 2001 にしない)');
  eq(computeCosts({ ...base, cost_incl_tax: 1100, fba_fee: 400, referral_fee_rate: 0.10, min_margin_rate: 0.12 }).floorPrice, 1924, '1500/0.78 = 1923.07… → 1924 (切り上げ)');
  // Codex R1 Medium: toFixed(6) は本物の端数まで消す。整数演算 (1/100 円 × basis point) なら 1600.01 / 0.8 = 2000.0125 → 2001
  eq(ceilDivide(1600.01, 0.8), 2001, '本物の端数 (1/100 円) は切り上げる (整数演算)');
  eq(ceilDivide(1600, 0.8), 2000, '割り切れるときは切り上げない');
  eq(ceilDivide(1600.004, 0.8), 2001, '1/100 円未満の端数も安全側 (切り上げ) に倒す');
  eq(ceilDivide(1600 + 1e-9, 0.8), 2000, '浮動小数の誤差 (1e-9) は切り上げない');
  eq(ceilDivide(1600, 0), null, '分母 0 は null');
  // Codex R3 Medium: 税抜 1003 × 1.10 = 1103.3 + 400 = 1503.3 / 0.8 = 1879.125 → 1880 (原価を整数に丸めると 1879 になる)
  eq(computeCosts({ ...base, cost_incl_tax: 1103.3 }).floorPrice, 1880, '原価の小数 (1103.3) を丸めずに下限 1880');

  console.log('  — Codex R1 High 2: 異常値は「有限の数」でも信用しない —');
  const neg = computeCosts({ ...base, cost_incl_tax: -100 });
  ok(neg.floorPrice === null && neg.flags.includes('INPUT_INVALID') && neg.flags.includes('COST_UNKNOWN'), '負の原価 → 下限 null + INPUT_INVALID');
  ok(computeCosts({ ...base, cost_incl_tax: 0 }).floorPrice === null, '原価 0 → 下限 null (0 円原価で下限を出さない)');
  // Codex R3 High: 「無い」は保守値で計算してよいが、「あるのに範囲外」は下限を出さない
  const zeroRate = computeCosts({ ...base, referral_fee_rate: 0 });
  ok(zeroRate.floorPrice === null && zeroRate.flags.includes('INPUT_INVALID') && zeroRate.feeRateValid === false, '手数料率 0 (範囲外) → 下限 null + INPUT_INVALID');
  ok(computeCosts({ ...base, referral_fee_rate: 0.9 }).floorPrice === null, '手数料率 90% (範囲外) → 下限 null');
  ok(computeCosts({ ...base, referral_fee_rate: null }).floorPrice === null, '手数料率が無い → 下限を出さない');
  eq(evaluateListing({ ...base, referral_fee_rate: 0, buybox_price: 1500 }).action, 'hold', '手数料率 0 + カートが安い → 保留 (以前は 1867 円への値下げが出た)');
  eq(evaluateListing({ ...base, referral_fee_rate: 0.9, buybox_price: 1500 }).action, 'hold', '手数料率 90% + カートが安い → 保留');
  eq(evaluateListing({ ...base, referral_fee_rate: 0, buybox_price: 2300 }).action, 'raise', '手数料率 0 でも値上げは出す');
  const noCh = computeCosts({ ...base, channel: null, ship_cost: 200 });
  ok(noCh.floorPrice === null && noCh.flags.includes('CHANNEL_UNKNOWN'), '発送区分不明 → 下限 null (FBM 扱いにしない)');
  eq(evaluateListing({ ...base, channel: null, ship_cost: 200, buybox_price: 1500 }).action, 'hold', '発送区分不明 + カートが安い → 保留 (以前は 1500 円への値下げが出た)');
  eq(evaluateListing({ ...base, channel: 'fbm', ship_cost: 200, buybox_price: 1400 }).reasonCode, 'FLOOR_CLAMP', '小文字の fbm は自己発送として通る (下限 1500 で止まる)');
  ok(computeCosts({ ...base, fba_fee: -1 }).floorPrice === null, '負の FBA 手数料 → 下限 null');
  ok(computeCosts({ ...base, per_item_fee: -5 }).floorPrice === null, '負の 1 品手数料 → 下限 null');
  ok(computeCosts({ ...base, cost_incl_tax: 99_999_999 }).floorPrice === null, '上限超えの原価 → 下限 null');
}

console.log('\n── 概算粗利 (grossAt) ──');
{
  const c = computeCosts(base);
  const g = grossAt(2000, c, 1000);
  eq(g.gross, 400, '2000 円で売ると粗利 400 (2000 - 1000 - 200 - 400)');
  ok(Math.abs(g.rate - 0.2) < 1e-9, '粗利率 20%');
  eq(grossAt(2000, computeCosts({ ...base, cost_incl_tax: null }), null).gross, null, '原価不明なら粗利は null');
}

console.log('\n── 追従しない (off) ──');
{
  const r = evaluateListing({ ...base, mode: 'off' });
  eq(r.action, 'keep', 'off は維持');
  eq(r.reasonCode, 'OFF', '  理由 OFF');
  const low = evaluateListing({ ...base, mode: 'off', my_price: 1500 });
  ok(low.flags.includes('BELOW_COST_FLOOR'), 'off でも今の価格が下限より低ければ旗 BELOW_COST_FLOOR');
  ok(low.reasonText.includes('⚠️'), '  理由文に注意が入る');
}

console.log('\n── カートに合わせる (buybox) 正常系 ──');
{
  const r = evaluateListing(base);
  eq(r.action, 'lower', 'カート 1900 < 自分 2000 → 値下げ');
  eq(r.proposedPrice, 1900, '  提案 1900');
  eq(r.reasonCode, 'MATCH_BUYBOX', '  理由 MATCH_BUYBOX');
  ok(r.confidence === 0.8, '  確信度 0.8');
  const up = evaluateListing({ ...base, buybox_price: 2300 });
  eq(up.action, 'raise', 'カート 2300 > 自分 2000 → 値上げ');
  eq(up.proposedPrice, 2300, '  提案 2300');
  const off = evaluateListing({ ...base, offset_jpy: -10 });
  eq(off.proposedPrice, 1890, '上乗せ −10 → 1890');
  const same = evaluateListing({ ...base, buybox_price: 2000 });
  eq(same.action, 'keep', 'カート = 自分 → 維持');
  eq(same.reasonCode, 'SAME', '  理由 SAME');
}

console.log('\n── ★事故ルール 1: 下限は「人の入力」と「計算値」の高い方 ──');
{
  const r = evaluateListing({ ...base, buybox_price: 1500 });
  eq(r.action, 'lower', 'カート 1500 だが下限 1750 → 1750 まで');
  eq(r.proposedPrice, 1750, '  提案 = 計算した下限');
  eq(r.reasonCode, 'FLOOR_CLAMP', '  理由 FLOOR_CLAMP');
  const withUser = evaluateListing({ ...base, buybox_price: 1500, floor_price: 1800 });
  eq(withUser.proposedPrice, 1800, '人のストッパー 1800 > 計算 1750 → 1800');
  const userLower = evaluateListing({ ...base, buybox_price: 1500, floor_price: 1600 });
  eq(userLower.proposedPrice, 1750, '人のストッパー 1600 < 計算 1750 → 計算値が勝つ');
  // ★旧ツールの事故: loss_stopper の既定 0 = 「下限なし」で競合最安値まで下げた
  const zero = evaluateListing({ ...base, buybox_price: 1500, floor_price: 0 });
  eq(zero.proposedPrice, 1750, '★ストッパー 0 (旧ツールの既定) でも計算した下限で止まる');
}

console.log('\n── ★事故ルール 2: 下限が分からない行は値下げしない ──');
{
  const r = evaluateListing({ ...base, cost_incl_tax: null, buybox_price: 1500 });
  eq(r.action, 'hold', '原価不明 + ストッパー無し + カートが安い → 保留');
  eq(r.reasonCode, 'NO_FLOOR', '  理由 NO_FLOOR');
  eq(r.proposedPrice, null, '  提案価格は出さない');
  const up = evaluateListing({ ...base, cost_incl_tax: null, buybox_price: 2300 });
  eq(up.action, 'raise', '原価不明でも値上げは出す');
  // Codex R4 High: 人のストッパーがあっても、計算した下限が無ければ値下げしない (旧ツールは人の入力だけで下げた)
  const withStopper = evaluateListing({ ...base, cost_incl_tax: null, buybox_price: 1500, floor_price: 1700 });
  eq(withStopper.action, 'hold', '★原価不明 + 人のストッパー 1700 + カートが安い → それでも保留');
  eq(withStopper.reasonCode, 'NO_FLOOR', '  理由 NO_FLOOR');
  for (const [label, patch] of [['原価不明', { cost_incl_tax: null }], ['FBA 手数料不明', { fba_fee: null }], ['FBA 手数料 0 (取得元の穴埋め)', { fba_fee: 0 }],
    ['送料不明 (自己発送)', { channel: 'FBM', ship_cost: null }], ['分母不正', { referral_fee_rate: 0.45, min_margin_rate: 0.6 }], ['発送区分不明', { channel: null }]]) {
    const r = evaluateListing({ ...base, ...patch, buybox_price: 1500, floor_price: 1700 });
    ok(r.action !== 'lower', `  ${label} + 人のストッパー → 値下げしない (${r.action} / ${r.reasonCode})`);
  }
  const fbaFeeUnknown = evaluateListing({ ...base, fba_fee: null, buybox_price: 1500 });
  eq(fbaFeeUnknown.action, 'hold', 'FBA 手数料不明でも同じく保留');
  // Codex R4 High 2: 取得元が FBAFees 欠落を 0 にする → FBA 手数料 0 は異常 (下限 1250 → 1500 への値下げを出さない)
  const fbaZero = evaluateListing({ ...base, fba_fee: 0, buybox_price: 1500 });
  eq(fbaZero.action, 'hold', 'FBA 手数料 0 + カートが安い → 保留');
  ok(computeCosts({ ...base, fba_fee: 0 }).flags.includes('INPUT_INVALID'), '  FBA 手数料 0 は INPUT_INVALID');
  eq(computeCosts({ ...base, channel: 'FBM', ship_cost: 0 }).floorPrice, 1250, '自己発送の送料 0 (同梱・無料) は正常');
}

console.log('\n── ★事故ルール 3: カート価格が半分未満は別物を疑う ──');
{
  const r = evaluateListing({ ...base, buybox_price: 900 });
  eq(r.action, 'hold', 'カート 900 < 2000 の半分 → 保留');
  eq(r.reasonCode, 'BUYBOX_SUSPICIOUS', '  理由 BUYBOX_SUSPICIOUS');
  const edge = evaluateListing({ ...base, buybox_price: 1000 });
  ok(edge.action !== 'hold' || edge.reasonCode !== 'BUYBOX_SUSPICIOUS', 'ちょうど半分は疑わない (下限で止まる)');
}

console.log('\n── ★事故ルール 4: 変更幅 −30% 〜 +100% / +10万円 ──');
{
  const big = evaluateListing({ ...base, buybox_price: 4100 });
  eq(big.action, 'hold', '+105% → 保留');
  eq(big.reasonCode, 'CHANGE_TOO_LARGE', '  理由 CHANGE_TOO_LARGE');
  const okUp = evaluateListing({ ...base, buybox_price: 4000 });
  eq(okUp.action, 'raise', '+100% ちょうどは通る');
  const bigAmt = evaluateListing({ ...base, my_price: 200000, buybox_price: 320000, cost_incl_tax: 100000 });
  eq(bigAmt.reasonCode, 'CHANGE_TOO_LARGE', '+12万円 → 保留');
  // −30% 超の値下げは、下限があれば下限で止まり、無ければ NO_FLOOR で止まる。下限 1750 < 2000×0.7=1400 にならないので
  // 下限を 1200 に下げた例で確認 (原価 500)
  const down = evaluateListing({ ...base, cost_incl_tax: 500, buybox_price: 1300 });
  eq(down.reasonCode, 'CHANGE_TOO_LARGE', '−35% → 保留 (下限 1125 より上でも変更幅で止まる)');
}

console.log('\n── Codex R1 High 1: 上限が下限より低い方針は矛盾として保留 ──');
{
  // 現在 2000 / 計算下限 1750 / 上限 1600 / カート 1500 → 以前は lower 1600 (下限割れ) が出ていた
  const r = evaluateListing({ ...base, buybox_price: 1500, ceiling_price: 1600 });
  eq(r.action, 'hold', '上限 1600 < 計算下限 1750 → 保留');
  eq(r.reasonCode, 'INVALID_POLICY_BOUNDS', '  理由 INVALID_POLICY_BOUNDS');
  eq(r.proposedPrice, null, '  提案価格は出さない');
  const r2 = evaluateListing({ ...base, buybox_price: 2300, ceiling_price: 1700, floor_price: 1800 });
  eq(r2.reasonCode, 'INVALID_POLICY_BOUNDS', '人のストッパー 1800 > 上限 1700 でも同じ (値上げ方向でも出さない)');
  // 不変条件: どの経路でも「値下げの提案 ≥ 実効下限」
  const cases = [];
  const unknownLower = [];
  let total = 0;
  for (const bb of [500, 1000, 1500, 1749, 1750, 1751, 1900, 2000, 2300, 2500, 4000]) {
    for (const ceiling of [null, 1600, 1750, 1800, 1900, 2200]) {
      for (const floor of [null, 0, 1600, 1800, 2100]) {
        for (const owner of [0, 1, null]) {
          for (const my of [1500, 2000]) {
            total += 1;
            const x = evaluateListing({ ...base, my_price: my, buybox_price: bb, ceiling_price: ceiling, floor_price: floor, buybox_is_mine: owner });
            if (x.action === 'lower' && (x.computedFloor == null || x.effectiveFloor == null || x.proposedPrice < x.effectiveFloor)) cases.push({ my, bb, ceiling, floor, owner, x: x.proposedPrice, f: x.effectiveFloor });
            if (x.action === 'lower' && owner === null) unknownLower.push({ my, bb, ceiling, floor, x: x.proposedPrice, code: x.reasonCode });
          }
        }
      }
    }
  }
  ok(cases.length === 0, `★総当たり ${total} 通りで「値下げの提案が実効下限を割る」が 0 件 (実際 ${cases.length}: ${JSON.stringify(cases.slice(0, 3))})`);
  ok(unknownLower.length === 0, `★総当たり ${total} 通りで「持ち主不明のカートで値下げ」が 0 件 (実際 ${unknownLower.length}: ${JSON.stringify(unknownLower.slice(0, 3))})`);
  // Codex R2 High 1 の再現: 持ち主不明 + カート 2300 (値上げ方向) + 上限 1900 → 以前は lower 1900 に反転していた
  const flip = evaluateListing({ ...base, buybox_price: 2300, buybox_is_mine: null, ceiling_price: 1900 });
  eq(flip.action, 'hold', '持ち主不明 + 上限で値上げが値下げに反転 → 保留');
  eq(flip.reasonCode, 'BUYBOX_OWNER_UNKNOWN', '  理由 BUYBOX_OWNER_UNKNOWN');
  const flipOther = evaluateListing({ ...base, buybox_price: 2300, buybox_is_mine: 0, ceiling_price: 1900 });
  eq(flipOther.action, 'lower', '持ち主が他社なら同じ形は上限 1900 への値下げ (1900 ≥ 下限 1750)');
}

console.log('\n── Codex R2 Medium 2: 小数の価格は丸めずに異常として扱う ──');
{
  eq(evaluateListing({ ...base, my_price: 0.6 }).reasonCode, 'NO_MY_PRICE', '自分の価格 0.6 は 1 円に丸めない → 保留');
  eq(evaluateListing({ ...base, my_price: 1999.5 }).reasonCode, 'NO_MY_PRICE', '自分の価格 1999.5 → 保留');
  eq(evaluateListing({ ...base, my_price: 9_999_999.4 }).reasonCode, 'NO_MY_PRICE', '上限直上の小数 → 保留');
  eq(evaluateListing({ ...base, buybox_price: 1899.5 }).reasonCode, 'NO_BUYBOX', 'カート 1899.5 → 保留');
  eq(evaluateListing({ ...base, my_price: 2000.0 }).action, 'lower', '2000.0 (整数値の REAL) は整数として通る');
}

console.log('\n── Codex R1 High 2: カートの持ち主が不明なら値下げしない ──');
{
  const r = evaluateListing({ ...base, buybox_is_mine: null, buybox_price: 1900 });
  eq(r.action, 'hold', '持ち主不明 + カートが安い → 保留 (自分を追いかけて下げ続けない)');
  eq(r.reasonCode, 'BUYBOX_OWNER_UNKNOWN', '  理由 BUYBOX_OWNER_UNKNOWN');
  const up = evaluateListing({ ...base, buybox_is_mine: null, buybox_price: 2300 });
  eq(up.action, 'raise', '持ち主不明でも値上げは出す');
  const same = evaluateListing({ ...base, buybox_is_mine: null, buybox_price: 2000 });
  eq(same.action, 'keep', '持ち主不明でカート = 自分 → 維持');
  const low = evaluateListing({ ...base, buybox_is_mine: null, buybox_price: 1400, my_price: 1500 });
  eq(low.reasonCode, 'RAISE_TO_FLOOR', '持ち主不明でカートが安くても、赤字なら下限まで上げる');
  eq(low.proposedPrice, 1750, '  提案 = 下限 1750');
  const upFromLoss = evaluateListing({ ...base, buybox_is_mine: null, buybox_price: 1900, my_price: 1500 });
  ok(upFromLoss.action === 'raise' && upFromLoss.proposedPrice === 1900, '持ち主不明でカートが高ければ普通の値上げ (1900)');
  eq(evaluateListing({ ...base, my_price: 10_000_000, buybox_price: 8_000_000 }).reasonCode, 'NO_MY_PRICE', '自分の価格が上限超え → 保留');
  eq(evaluateListing({ ...base, my_price: -5 }).reasonCode, 'NO_MY_PRICE', '自分の価格が負 → 保留');
  eq(evaluateListing({ ...base, buybox_price: -1 }).reasonCode, 'NO_BUYBOX', 'カート価格が負 → 保留');
  eq(evaluateListing({ ...base, floor_price: -100, buybox_price: 1500 }).proposedPrice, 1750, '負のストッパーは無視して計算下限が効く');
  eq(evaluateListing({ ...base, offset_jpy: -999_999 }).proposedPrice, 1900, '異常な上乗せ (−99万) は 0 扱い');
}

console.log('\n── 高値ストッパー / カート自社 / カート無し ──');
{
  const c = evaluateListing({ ...base, buybox_price: 2300, ceiling_price: 2200 });
  eq(c.proposedPrice, 2200, '上限 2200 で止まる');
  eq(c.reasonCode, 'CEILING_CLAMP', '  理由 CEILING_CLAMP');
  const mine = evaluateListing({ ...base, buybox_is_mine: 1, buybox_price: 2000 });
  eq(mine.action, 'keep', 'カート自社 → 維持');
  eq(mine.reasonCode, 'BUYBOX_MINE', '  理由 BUYBOX_MINE');
  const mineLow = evaluateListing({ ...base, buybox_is_mine: 1, my_price: 1500, buybox_price: 1500 });
  eq(mineLow.action, 'raise', 'カート自社でも下限より低ければ下限まで上げる');
  eq(mineLow.proposedPrice, 1750, '  提案 = 下限');
  eq(mineLow.reasonCode, 'RAISE_TO_FLOOR', '  理由 RAISE_TO_FLOOR');
  const none = evaluateListing({ ...base, buybox_price: null, buybox_is_mine: null });
  eq(none.action, 'hold', 'カート無し → 保留');
  eq(none.reasonCode, 'NO_BUYBOX', '  理由 NO_BUYBOX');
  const noneLow = evaluateListing({ ...base, buybox_price: null, buybox_is_mine: null, my_price: 1500 });
  eq(noneLow.reasonCode, 'RAISE_TO_FLOOR', 'カート無しでも赤字なら下限まで上げる');
}

console.log('\n── カスタム (型で決める) ──');
{
  const T = (over = {}) => ({ type_id: 1, name: 'テスト型', basis: 'buybox', rival_scope: 'all', direction: 'both', offset_kind: 'jpy', offset_value: 0,
    amazon_seller: 'include', prime_as: 'fba', points: 'price_only', solo_raise: 'none', archived_at: null, ...over });
  eq(evaluateListing({ ...base, mode: 'custom', custom: null }).reasonCode, 'NO_CUSTOM_TYPE', '型が無ければ保留');
  eq(evaluateListing({ ...base, mode: 'custom', custom: T({ archived_at: '2026-09-10T00:00:00.000Z' }) }).reasonCode, 'NO_CUSTOM_TYPE', '「使わない」型なら保留');
  const plain = evaluateListing({ ...base, mode: 'custom', custom: T() });
  ok(plain.action === 'lower' && plain.proposedPrice === 1900 && plain.reasonCode === 'MATCH_BUYBOX', `基準カート・上下・上乗せ 0 → buybox と同じ (${plain.action} ${plain.proposedPrice})`);
  ok(plain.reasonText.includes('型「テスト型」'), '  理由文に型の名前が入る');
  eq(evaluateListing({ ...base, mode: 'custom', custom: T({ offset_kind: 'jpy', offset_value: -10 }), offset_jpy: 50 }).proposedPrice, 1890, '上乗せは型の値 (−10 円) が方針の offset_jpy (+50) より優先');
  eq(evaluateListing({ ...base, mode: 'custom', custom: T({ offset_kind: 'pct', offset_value: -2 }) }).proposedPrice, 1862, '上乗せ −2% → 1900 × 0.98 = 1862');
  eq(evaluateListing({ ...base, mode: 'custom', custom: T({ offset_kind: 'pct', offset_value: 3 }), buybox_price: 1999 }).proposedPrice, 2059, '上乗せ +3% → 1999 × 1.03 = 2058.97 → 2059 (四捨五入)');
  const up = evaluateListing({ ...base, mode: 'custom', custom: T({ direction: 'up_only' }) });
  ok(up.action === 'keep' && up.reasonCode === 'DIRECTION_UP_ONLY' && up.targetPrice === 1900, `値上げのみ + カートが安い → 維持 (${up.reasonCode})`);
  eq(evaluateListing({ ...base, mode: 'custom', custom: T({ direction: 'up_only' }), buybox_price: 2300 }).action, 'raise', '  値上げのみ + カートが高い → 値上げは出す');
  eq(evaluateListing({ ...base, mode: 'custom', custom: T({ direction: 'up_only' }), buybox_price: 2300, ceiling_price: 2200 }).proposedPrice, 2200, '  値上げのみ + 上限 2200 → 2200 で止める');
  const down = evaluateListing({ ...base, mode: 'custom', custom: T({ direction: 'down_only' }), buybox_price: 2300 });
  ok(down.action === 'keep' && down.reasonCode === 'DIRECTION_DOWN_ONLY', `値下げのみ + カートが高い → 維持 (${down.reasonCode})`);
  eq(evaluateListing({ ...base, mode: 'custom', custom: T({ direction: 'down_only' }) }).action, 'lower', '  値下げのみ + カートが安い → 値下げは出す');
  const belowFloor = evaluateListing({ ...base, mode: 'custom', custom: T({ direction: 'down_only' }), my_price: 1500, buybox_price: null, buybox_is_mine: null });
  ok(belowFloor.action === 'raise' && belowFloor.reasonCode === 'RAISE_TO_FLOOR' && belowFloor.proposedPrice === 1750, `★値下げのみでも、赤字の疑いなら下限まで上げる (${belowFloor.reasonCode} → ${belowFloor.proposedPrice})`);
  // Codex R1 P1: 他社カート 1600 に合わせて下限 1750 で止まる経路 (FLOOR_CLAMP) でも、赤字なら値下げのみの型に止められない
  const clampRaise = evaluateListing({ ...base, mode: 'custom', custom: T({ direction: 'down_only' }), my_price: 1500, buybox_price: 1600, buybox_is_mine: 0 });
  ok(clampRaise.action === 'raise' && clampRaise.proposedPrice === 1750 && clampRaise.reasonCode === 'FLOOR_CLAMP', `★Codex R1: 値下げのみ + 他社カート 1600 + 自分 1500 (赤字) → 下限 1750 まで上げる (${clampRaise.reasonCode} → ${clampRaise.proposedPrice})`);
  const aboveFloor = evaluateListing({ ...base, mode: 'custom', custom: T({ direction: 'down_only' }), my_price: 1800, buybox_price: 1900, buybox_is_mine: 0 });
  ok(aboveFloor.action === 'keep' && aboveFloor.reasonCode === 'DIRECTION_DOWN_ONLY', `  赤字でなければ (1800 ≥ 1750) 値下げのみは値上げを止める (${aboveFloor.reasonCode})`);
  for (const [label, over] of [['最安値', { basis: 'lowest' }], ['Amazon 本体を無視', { amazon_seller: 'ignore' }], ['実質価格', { points: 'effective' }], ['独占時の値上げ', { solo_raise: 'to_ceiling' }]]) {
    const r = evaluateListing({ ...base, mode: 'custom', custom: T(over) });
    ok(r.action === 'hold' && r.reasonCode === 'CUSTOM_NEEDS_OFFERS' && r.reasonText.includes(label), `${label} を含む型は保留 (${r.reasonCode})`);
  }
  ok(customNeedsOffers(T()).length === 0 && customNeedsOffers(T({ basis: 'lowest', rival_scope: 'fba' })).join('').includes('FBA のみ'), 'customNeedsOffers: 配送設定は最安値のときだけ札に出る');
  eq(customTypeSummary(T({ direction: 'up_only', offset_kind: 'jpy', offset_value: -10 })), 'カート価格・値上げのみ・−10円', '型の 1 行要約');
  const n = normalizeCustomType({ basis: 'なにか', direction: 'up_only', offset_kind: 'pct', offset_value: 500, name: 'x' });
  ok(n.basis === 'buybox' && n.direction === 'up_only' && n.offset_value === 0, '知らない値は既定に、範囲外の上乗せは 0 に戻す');
  eq(normalizeCustomType(null), null, 'null は null');
  const inputs = describeInputs({}, { mode: 'custom', custom_type_id: 1, custom_type: T() }, '2026-09-10');
  ok(inputs.policy.custom_type_id === 1 && inputs.policy.custom_type.name === 'テスト型', 'inputs に型の番号と判定時点の中身が写る');
  ok(['NO_CUSTOM_TYPE', 'CUSTOM_NEEDS_OFFERS', 'DIRECTION_UP_ONLY', 'DIRECTION_DOWN_ONLY'].every((k) => typeof REASONS[k] === 'string'), '新しい理由コードに日本語の説明がある');
}

console.log('\n── まだ判定できないもの ──');
{
  eq(evaluateListing({ ...base, mode: 'fba_lowest' }).reasonCode, 'NO_OFFER_DATA', 'FBA最安値モードは Phase 2 まで保留');
  eq(evaluateListing({ ...base, mode: 'lowest' }).reasonCode, 'NO_OFFER_DATA', '最安値モードも同じ');
  eq(evaluateListing({ ...base, my_price: null }).reasonCode, 'NO_MY_PRICE', '自分の価格が無ければ保留');
  eq(evaluateListing({ ...base, mode: 'なにか' }).reasonCode, 'OFF', '知らないモードは off 扱い');
}

console.log('\n── 判定の付属情報 ──');
{
  const r = evaluateListing({ ...base, units_30d: 0 });
  ok(r.flags.includes('NO_SALES_30D'), '30日売れていない旗');
  ok(RULE_VERSION.startsWith('rule:'), 'ルール版が付く');
  ok(Object.keys(REASONS).every((k) => typeof REASONS[k] === 'string' && REASONS[k].length > 0), 'すべての理由コードに日本語の説明がある');
  ok(Object.keys(FLAGS).every((k) => typeof FLAGS[k] === 'string'), 'すべての旗に日本語の説明がある');
  ok(Object.keys(ACTIONS).length === 4 && Object.keys(MODES).length === 5, '判定 4 種・モード 5 種 (custom を含む)');
  const inputs = describeInputs({ my_price: null, buybox_price: 1900, units_30d: null }, null, '2026-09-07');
  ok(inputs.my_price === null && inputs.units_30d === null, 'inputs は null を null のまま残す (0 にしない)');
  ok(inputs.rule_version === RULE_VERSION && inputs.snapshot_date_jst === '2026-09-07', 'inputs にルール版と日付が入る');
  ok(!isValidPrice(0) && !isValidPrice(-5) && !isValidPrice(1.5) && isValidPrice(1), '価格の妥当性');
}

console.log(`\n${failed === 0 ? '🎉 ALL PASS' : `❌ ${failed} 件失敗`}`);
process.exit(failed === 0 ? 0 : 1);
