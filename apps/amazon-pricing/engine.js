/**
 * engine.js — Amazon 価格管理 (自社プライスター) の判定エンジン。**純関数だけ** (DB も fetch も触らない)。
 *
 * ここで決めるのは「もし価格を変えるなら、いくらに、なぜ」まで。
 * 実際に Amazon の価格を変える処理は、このアプリのどこにも無い (README「書き込み経路が無いことの保証」)。
 *
 * ★2026-03 の事故 (旧ツールの自動改定で誤った価格を送った) から決めたルール:
 *   1. 下限 (赤字ストッパー) は「人が入れた値」だけに頼らない。原価と手数料から**計算した下限**を必ず併用し、
 *      **高い方**を使う。人の入力が空でも原価割れの提案は出ない
 *      (旧ツールは loss_stopper の既定が 0 = 「下限なし」で、競合の最安値まで無条件に下げられた)
 *   2. 下限を計算できない行 (原価不明・FBA手数料不明・自己発送の送料不明) では**値下げを提案しない** (hold)。
 *      値上げは上限まで許す
 *   3. カート価格が自分の価格の半分未満なら「別コンディション / セット崩れ / 取得ミス」を疑って止める
 *   4. 変更幅は −30% 〜 +100%、+10万円まで (価格一括改定ツールと同じ数字)
 *   5. 判定には必ず reason_code と、人が読める reason_text と、参照した入力 (inputs) を付ける。
 *      後から AI と人が「なぜそう判断したか」を検証できるようにするため (Company DB 方針 6)
 *
 * 入力の単位: 価格はすべて税込・整数円。原価 (cost_incl_tax) は税込換算済み。手数料率は 0.10 のような小数。
 */

export const RULE_VERSION = 'rule:ap-v1';

/** 追従モード (旧ツール・プライスターの語彙を英語キーに直したもの) */
export const MODES = {
  off: '追従しない',
  buybox: 'カートに合わせる',
  fba_lowest: 'FBA最安値に合わせる',
  lowest: '最安値に合わせる',
};
export const MODE_KEYS = Object.keys(MODES);

/** 既定の最低粗利率 (下限の計算に使う)。policy.min_margin_rate が無い行に効く */
export const DEFAULT_MIN_MARGIN_RATE = 0.10;
/** 手数料率が取れていない時の保守値 (高めに見ておく = 下限が高くなる方向で安全) */
export const FALLBACK_REFERRAL_RATE = 0.15;
/** 変更幅のガード (価格一括改定ツール pricing.js と同じ数字) */
export const MIN_CHANGE_RATIO = -0.30;
export const MAX_CHANGE_RATIO = 1.00;
export const MAX_CHANGE_AMOUNT = 100_000;
/** カート価格が自分の価格のこの割合を下回ったら「別物の疑い」で止める */
export const SUSPICIOUS_BUYBOX_RATIO = 0.5;
/** 価格の上限 (これを超える値は扱わない) */
export const MAX_PRICE = 9_999_999;

/** 判定の種類 */
export const ACTIONS = { raise: '値上げ', lower: '値下げ', keep: '維持', hold: '保留' };

/** 理由コード → 人が読む説明 (画面と AI の両方がこの表を見る) */
export const REASONS = {
  OFF: '追従しない設定',
  NO_MY_PRICE: '自分の出品価格が取れていない (価格スナップショットに無い)',
  NO_BUYBOX: 'カート価格が無い (カート不在 or 未取得)',
  BUYBOX_MINE: 'カートは自社が持っている → 今の価格を維持',
  NO_OFFER_DATA: '競合の最安値はまだ取得していない (Phase 2) → 判定できない',
  NO_FLOOR: '下限を計算できない (原価・手数料・送料のどれかが不明) → 値下げは保留',
  BUYBOX_SUSPICIOUS: 'カート価格が自分の価格の半分未満 → 別コンディション・セット崩れ・取得ミスの疑い',
  CHANGE_TOO_LARGE: '変更幅が大きすぎる (−30%〜+100% / +10万円を超える) → 保留',
  INVALID_TARGET: '目標価格が不正 (0 以下・上限超え)',
  SAME: '目標が今の価格と同じ → 維持',
  MATCH_BUYBOX: 'カート価格に合わせる',
  FLOOR_CLAMP: 'カートに合わせると下限を割るので、下限で止める',
  CEILING_CLAMP: 'カートに合わせると上限を超えるので、上限で止める',
  RAISE_TO_FLOOR: '今の価格が下限より低い (赤字の疑い) → 下限まで上げる',
};

/** 判定に付ける旗 (reason とは別に、注意として画面に出す) */
export const FLAGS = {
  BELOW_COST_FLOOR: '今の価格が原価+手数料から計算した下限より低い (赤字の疑い)',
  BELOW_STOPPER: '今の価格が赤字ストッパーより低い',
  COST_UNKNOWN: '原価が未登録 (NE商品マスタ)',
  FEE_RATE_ASSUMED: '販売手数料率が取れていないので 15% と仮定',
  FBA_FEE_UNKNOWN: 'FBA配送代行手数料が取れていない',
  SHIP_UNKNOWN: '自己発送の送料が未登録',
  FLOOR_CLAMP: '下限で止めた',
  CEILING_CLAMP: '上限で止めた',
  NO_SALES_30D: '直近30日 (確定分) の販売なし',
};

/** null 安全な数値化。'' / null / undefined は null のまま (Number(null) は 0 になるので使わない) */
export function num(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/** 税込・整数円として妥当か */
export function isValidPrice(v) {
  return Number.isInteger(v) && v > 0 && v <= MAX_PRICE;
}

/**
 * 原価・手数料から「最低粗利率を守れる下限価格」を出す。
 *
 *   下限 = ceil( (原価税込 + 固定費) / (1 − 販売手数料率 − 最低粗利率) )
 *   固定費 = FBA なら FBA配送代行手数料、自己発送なら送料。それに 1品ごとの手数料・成約料を足す
 *
 * @param {object} p
 * @param {'FBA'|'FBM'|string|null} p.channel
 * @param {number|null} p.cost_incl_tax      税込換算済みの原価 (セットは構成品の合計)
 * @param {number|null} p.cost_missing_parts 原価が無い構成品の数 (>0 なら原価不明)
 * @param {number|null} p.referral_fee_rate
 * @param {number|null} p.fba_fee
 * @param {number|null} p.per_item_fee
 * @param {number|null} p.variable_closing_fee
 * @param {number|null} p.ship_cost          自己発送の送料 (NE商品マスタ)
 * @param {number|null} p.min_margin_rate    最低粗利率 (null = 既定)
 * @returns {{costKnown:boolean, feeRate:number, feeRateAssumed:boolean, fixedFees:number|null,
 *            minMarginRate:number, floorPrice:number|null, flags:string[]}}
 */
export function computeCosts(p) {
  const flags = [];
  const cost = num(p.cost_incl_tax);
  const missingParts = num(p.cost_missing_parts) ?? 0;
  const isFba = String(p.channel || '').toUpperCase() === 'FBA';
  const minMarginRate = num(p.min_margin_rate) ?? DEFAULT_MIN_MARGIN_RATE;

  let feeRate = num(p.referral_fee_rate);
  let feeRateAssumed = false;
  if (feeRate == null) { feeRate = FALLBACK_REFERRAL_RATE; feeRateAssumed = true; flags.push('FEE_RATE_ASSUMED'); }

  let costKnown = cost != null && missingParts === 0;
  if (!costKnown) flags.push('COST_UNKNOWN');

  // 固定費。不明なものがあれば「下限は計算できない」に倒す (0 で埋めると下限が低く出て、値下げを通してしまう)
  let fixedFees = 0;
  let fixedKnown = true;
  if (isFba) {
    const fba = num(p.fba_fee);
    if (fba == null) { fixedKnown = false; flags.push('FBA_FEE_UNKNOWN'); } else fixedFees += fba;
  } else {
    const ship = num(p.ship_cost);
    if (ship == null) { fixedKnown = false; flags.push('SHIP_UNKNOWN'); } else fixedFees += ship;
  }
  fixedFees += num(p.per_item_fee) ?? 0;
  fixedFees += num(p.variable_closing_fee) ?? 0;

  let floorPrice = null;
  const denom = 1 - feeRate - minMarginRate;
  if (costKnown && fixedKnown && denom > 0) {
    // ★浮動小数の誤差で 2000.0000000000002 → 2001 と 1 円ずれるので、6 桁で丸めてから切り上げる
    floorPrice = Math.ceil(Number(((cost + fixedFees) / denom).toFixed(6)));
  }
  return {
    costKnown, feeRate, feeRateAssumed,
    fixedFees: fixedKnown ? fixedFees : null,
    minMarginRate, floorPrice, flags,
  };
}

/**
 * その価格で売れたときの概算粗利 (参考表示用。請求実額ではない)。
 * 原価か固定費が不明なら null (0 円原価として計算しない)。
 */
export function grossAt(price, costs, costInclTax) {
  const p = num(price);
  const c = num(costInclTax);
  if (p == null || c == null || !costs.costKnown || costs.fixedFees == null) return { gross: null, rate: null };
  const gross = Math.round(p - c - p * costs.feeRate - costs.fixedFees);
  return { gross, rate: p > 0 ? gross / p : null };
}

/**
 * 1 出品ぶんの判定。
 *
 * @param {object} input
 * @param {string} input.mode                 'off'|'buybox'|'fba_lowest'|'lowest'
 * @param {number|null} input.my_price        今の出品価格 (税込)
 * @param {number|null} input.buybox_price
 * @param {0|1|null} input.buybox_is_mine     1=自社保有 / 0=他社 / null=不明・カート不在
 * @param {number|null} input.floor_price     人が入れた赤字ストッパー
 * @param {number|null} input.ceiling_price   人が入れた高値ストッパー
 * @param {number} [input.offset_jpy]         目標への上乗せ (−10 = カートより 10 円安く)
 * @param {number|null} [input.units_30d]
 * @param {...} computeCosts の入力もそのまま受ける
 * @returns {{action:string, proposedPrice:number|null, targetPrice:number|null, currentPrice:number|null,
 *            reasonCode:string, reasonText:string, confidence:number|null, flags:string[],
 *            computedFloor:number|null, effectiveFloor:number|null, changeRatio:number|null,
 *            costs:object, grossNow:{gross:number|null, rate:number|null}}}
 */
export function evaluateListing(input) {
  const mode = MODE_KEYS.includes(input.mode) ? input.mode : 'off';
  const costs = computeCosts(input);
  const flags = [...costs.flags];
  const currentRaw = num(input.my_price);
  const current = currentRaw == null ? null : Math.round(currentRaw);
  const grossNow = grossAt(current, costs, input.cost_incl_tax);
  const userFloor = num(input.floor_price);
  const ceiling = num(input.ceiling_price);
  const offset = num(input.offset_jpy) ?? 0;
  const units30 = num(input.units_30d);
  if (units30 == null || units30 <= 0) flags.push('NO_SALES_30D');

  // 実効下限 = 人の入力と計算値の高い方。どちらも無ければ null (= 下限が分からない)
  let effectiveFloor = null;
  if (userFloor != null && costs.floorPrice != null) effectiveFloor = Math.max(userFloor, costs.floorPrice);
  else effectiveFloor = userFloor ?? costs.floorPrice ?? null;

  if (current != null) {
    if (costs.floorPrice != null && current < costs.floorPrice) flags.push('BELOW_COST_FLOOR');
    if (userFloor != null && current < userFloor) flags.push('BELOW_STOPPER');
  }

  const base = {
    currentPrice: current, computedFloor: costs.floorPrice, effectiveFloor, costs, grossNow, flags,
    targetPrice: null, proposedPrice: null, changeRatio: null,
  };
  const hold = (code, extra = '') => ({
    ...base, action: 'hold', reasonCode: code, reasonText: REASONS[code] + extra, confidence: null,
  });
  const keep = (code, conf, extra = '') => ({
    ...base, action: 'keep', reasonCode: code, reasonText: REASONS[code] + extra, confidence: conf,
  });

  if (current == null) return hold('NO_MY_PRICE');

  if (mode === 'off') {
    const extra = flags.includes('BELOW_COST_FLOOR')
      ? ` (⚠️ 今の価格 ${current.toLocaleString()} 円は計算した下限 ${costs.floorPrice.toLocaleString()} 円より低い)` : '';
    return keep('OFF', null, extra);
  }
  if (mode === 'fba_lowest' || mode === 'lowest') return hold('NO_OFFER_DATA');

  // ── mode === 'buybox' ──
  const bb = num(input.buybox_price);
  let target;
  let code;
  if (bb == null) {
    // カート不在でも、赤字の疑いがあれば下限まで上げる提案はできる
    if (effectiveFloor != null && current < effectiveFloor) { target = effectiveFloor; code = 'RAISE_TO_FLOOR'; }
    else return hold('NO_BUYBOX');
  } else if (input.buybox_is_mine === 1) {
    if (effectiveFloor != null && current < effectiveFloor) { target = effectiveFloor; code = 'RAISE_TO_FLOOR'; }
    else return keep('BUYBOX_MINE', 0.9);
  } else if (bb < current * SUSPICIOUS_BUYBOX_RATIO) {
    return hold('BUYBOX_SUSPICIOUS', ` (カート ${Math.round(bb).toLocaleString()} 円 / 自分 ${current.toLocaleString()} 円)`);
  } else {
    target = Math.round(bb + offset);
    code = 'MATCH_BUYBOX';
  }

  // 下限・上限で止める
  if (effectiveFloor != null && target < effectiveFloor) {
    target = effectiveFloor; code = 'FLOOR_CLAMP'; flags.push('FLOOR_CLAMP');
  }
  if (effectiveFloor == null && target < current) {
    // 下限が分からない行の値下げは出さない (事故ルール 2)
    return hold('NO_FLOOR', ` (カートは ${Math.round(bb).toLocaleString()} 円)`);
  }
  if (ceiling != null && target > ceiling) {
    target = ceiling; code = 'CEILING_CLAMP'; flags.push('CEILING_CLAMP');
  }
  if (!isValidPrice(target)) return hold('INVALID_TARGET', ` (${target})`);

  if (target === current) return { ...keep(code === 'MATCH_BUYBOX' ? 'SAME' : code, 0.9), targetPrice: target };

  const changeRatio = (target - current) / current;
  const changeAmount = target - current;
  if (changeRatio < MIN_CHANGE_RATIO || changeRatio > MAX_CHANGE_RATIO || changeAmount > MAX_CHANGE_AMOUNT) {
    return { ...hold('CHANGE_TOO_LARGE', ` (${current.toLocaleString()} → ${target.toLocaleString()} 円, ${(changeRatio * 100).toFixed(1)}%)`), targetPrice: target, changeRatio };
  }

  const confidence = code === 'RAISE_TO_FLOOR' ? 0.9
    : code === 'MATCH_BUYBOX' ? (costs.feeRateAssumed ? 0.6 : 0.8)
      : 0.7; // FLOOR_CLAMP / CEILING_CLAMP
  const action = target > current ? 'raise' : 'lower';
  const reasonText = `${REASONS[code]} (${current.toLocaleString()} → ${target.toLocaleString()} 円, ${changeRatio >= 0 ? '+' : ''}${(changeRatio * 100).toFixed(1)}%)`;
  return {
    ...base, action, targetPrice: target, proposedPrice: target, changeRatio,
    reasonCode: code, reasonText, confidence,
  };
}

/**
 * 判定に使った入力を、そのまま監査に残せる形に整える (どの表のどの日の値か)。
 * 数値は丸めない。null は null のまま (「無かった」と「0 だった」を区別するため)。
 */
export function describeInputs(row, policy, snapshotDate) {
  return {
    sources: {
      my_price: 'mirror_amazon_price_snapshot_daily.my_price',
      buybox: 'mirror_amazon_price_snapshot_daily.buybox_price / buybox_is_mine',
      fees: 'mirror_amazon_sku_fees',
      cost: 'mirror_sku_resolved × mirror_products (原価 × (1+消費税率) × 数量)',
      sales: 'mirror_amazon_finance_sku_daily (直近30日, 確定分)',
      policy: 'ap_policies',
    },
    snapshot_date_jst: snapshotDate ?? null,
    my_price: num(row.my_price), buybox_price: num(row.buybox_price), buybox_is_mine: row.buybox_is_mine ?? null,
    channel: row.channel ?? null,
    referral_fee_rate: num(row.referral_fee_rate), fba_fee: num(row.fba_fee),
    per_item_fee: num(row.per_item_fee), variable_closing_fee: num(row.variable_closing_fee),
    cost_incl_tax: num(row.cost_incl_tax), cost_missing_parts: num(row.cost_missing_parts), ship_cost: num(row.ship_cost),
    units_30d: num(row.units_30d),
    policy: policy ? {
      mode: policy.mode, floor_price: policy.floor_price, ceiling_price: policy.ceiling_price,
      offset_jpy: policy.offset_jpy, min_margin_rate: policy.min_margin_rate, updated_at: policy.updated_at,
    } : null,
    rule_version: RULE_VERSION,
  };
}
