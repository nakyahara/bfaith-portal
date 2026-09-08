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
 *   2. 下限を計算できない行 (原価不明・FBA手数料不明・自己発送の送料不明・入力が異常) では**値下げを提案しない** (hold)。
 *      人の赤字ストッパーがあっても免除しない (原価を知らずに下げてよい値は決められない)。値上げは上限まで許す
 *   3. カート価格が自分の価格の半分未満なら「別コンディション / セット崩れ / 取得ミス」を疑って止める
 *   4. 変更幅は −30% 〜 +100%、+10万円まで (価格一括改定ツールと同じ数字)
 *   5. 判定には必ず reason_code と、人が読める reason_text と、参照した入力 (inputs) を付ける。
 *      後から AI と人が「なぜそう判断したか」を検証できるようにするため (Company DB 方針 6)
 *   6. (Codex R1) 入力は「有限の数」というだけでは信用しない。原価・手数料・価格ごとに取りうる範囲を決め、
 *      外れた行は下限を計算しない = 値下げしない。カートの持ち主が不明なら他社扱いにしない (自分を追いかけて下げ続けるのを防ぐ)
 *   7. (Codex R1) 上限が実効下限より低い方針は矛盾 → 保留。どの経路でも「値下げの提案は実効下限以上」を最後に検査する
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
/** 手数料率が取れていない・範囲外の時の保守値 (高めに見ておく = 下限が高くなる方向で安全) */
export const FALLBACK_REFERRAL_RATE = 0.15;
/** 販売手数料率として信用する範囲 (Amazon は 6〜20% 程度。0 や 50% 超はデータ異常) */
export const REFERRAL_RATE_MIN = 0.01;
export const REFERRAL_RATE_MAX = 0.50;
/** 変更幅のガード (価格一括改定ツール pricing.js と同じ数字) */
export const MIN_CHANGE_RATIO = -0.30;
export const MAX_CHANGE_RATIO = 1.00;
export const MAX_CHANGE_AMOUNT = 100_000;
/** カート価格が自分の価格のこの割合を下回ったら「別物の疑い」で止める */
export const SUSPICIOUS_BUYBOX_RATIO = 0.5;
/** 価格・原価・手数料の上限 (これを超える値は扱わない) */
export const MAX_PRICE = 9_999_999;
export const MAX_COST = 9_999_999;
export const MAX_FEE = 999_999;

/** 判定の種類 */
export const ACTIONS = { raise: '値上げ', lower: '値下げ', keep: '維持', hold: '保留' };

/** 理由コード → 人が読む説明 (画面と AI の両方がこの表を見る) */
export const REASONS = {
  OFF: '追従しない設定',
  NO_MY_PRICE: '自分の出品価格が取れていない・異常 (価格スナップショットに無い / 0 以下 / 上限超え)',
  NO_BUYBOX: 'カート価格が無い・異常 (カート不在 or 未取得)',
  BUYBOX_MINE: 'カートは自社が持っている → 今の価格を維持',
  BUYBOX_OWNER_UNKNOWN: 'カートの持ち主が分からない (自社かもしれない) → 値下げは保留',
  NO_OFFER_DATA: '競合の最安値はまだ取得していない (Phase 2) → 判定できない',
  NO_FLOOR: '下限を計算できない (原価・手数料・送料のどれかが不明か異常) → 値下げは保留',
  INVALID_POLICY_BOUNDS: '高値ストッパーが実効下限より低い (方針が矛盾) → 保留',
  BUYBOX_SUSPICIOUS: 'カート価格が自分の価格の半分未満 → 別コンディション・セット崩れ・取得ミスの疑い',
  CHANGE_TOO_LARGE: '変更幅が大きすぎる (−30%〜+100% / +10万円を超える) → 保留',
  INVALID_TARGET: '目標価格が不正 (0 以下・上限超え)',
  FLOOR_INVARIANT: '内部検査: 値下げの提案が下限を割った → 保留 (ルールの不具合。報告してください)',
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
  INPUT_INVALID: '入力に異常値がある (負の原価・範囲外の手数料率など。データ側を確認)',
  CHANNEL_UNKNOWN: '発送区分が FBA / 自己発送のどちらか分からない',
  FEE_RATE_ASSUMED: '販売手数料率が取れていない・範囲外 (概算粗利は 15% で仮計算。下限は出さない = 値下げしない)',
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

/** 金額 (原価・手数料) として取りうる範囲か。null は「無い」、範囲外は「異常」として呼び出し側が区別する */
function amountState(v, max, { allowZero = true } = {}) {
  const n = num(v);
  if (n == null) return { value: null, state: 'missing' };
  if (n < 0 || n > max || (!allowZero && n === 0)) return { value: n, state: 'invalid' };
  return { value: n, state: 'ok' };
}

/**
 * 下限 = ceil((原価税込 + 固定費) / (1 − 販売手数料率 − 最低粗利率)) を、浮動小数の誤差を持ち込まずに出す。
 * 金額は 1/100 円、率は basis point (1/10000) の整数に直して割る。整数 ÷ 整数の ceil は 2^53 未満なら正確。
 * ★分解能は意図して 1/100 円: 入力 (原価 × 税率 × 数量・手数料) は最大でも小数 2 桁で、それ未満の端数は
 *   浮動小数の誤差でしか生まれない (toFixed(6) のような「なんとなくの桁」ではなく、データの桁に合わせた)
 */
export function ceilDivide(amountYen, ratio) {
  // どちらも「下限が高くなる側」へ丸める (金額は切り上げ、分母は切り捨て — Codex R3 Medium)
  const n = Math.ceil(amountYen * 100 - 1e-7);    // 1/100 円
  const bp = Math.floor(ratio * 10000 + 1e-7);    // basis point
  if (bp <= 0) return null;
  return Math.ceil((n * 10000) / (bp * 100));
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
  // 発送区分は FBA / FBM の 2 値だけ信用する。不明なら固定費が決められない → 下限を出さない (Codex R3 High)
  const channel = String(p.channel || '').toUpperCase();
  const isFba = channel === 'FBA';
  const channelKnown = channel === 'FBA' || channel === 'FBM';
  if (!channelKnown) flags.push('CHANNEL_UNKNOWN');
  let minMarginRate = num(p.min_margin_rate) ?? DEFAULT_MIN_MARGIN_RATE;
  if (minMarginRate < 0 || minMarginRate >= 0.9) minMarginRate = DEFAULT_MIN_MARGIN_RATE;

  // 原価: 無い・0 以下・上限超え・構成品に欠け → 不明 (0 円原価で下限を出さない)
  const cost = amountState(p.cost_incl_tax, MAX_COST, { allowZero: false });
  const missingParts = num(p.cost_missing_parts) ?? 0;
  const costKnown = cost.state === 'ok' && missingParts === 0;
  if (!costKnown) flags.push('COST_UNKNOWN');
  if (cost.state === 'invalid') flags.push('INPUT_INVALID');

  // 販売手数料率: 無い・範囲外 (0・負・50% 超) のどちらも **下限は出さない** (= 値下げしない)。
  // 15% の仮定は概算粗利の**表示**にだけ使う (Codex R5 High: 15% は安全側とは限らない。20% の商品なら下限が 133 円低く出る)
  let feeRate = num(p.referral_fee_rate);
  let feeRateAssumed = false;
  let feeRateValid = true;
  if (feeRate == null) {
    feeRateValid = false; feeRate = FALLBACK_REFERRAL_RATE; feeRateAssumed = true; flags.push('FEE_RATE_ASSUMED');
  } else if (feeRate < REFERRAL_RATE_MIN || feeRate > REFERRAL_RATE_MAX) {
    feeRateValid = false; flags.push('INPUT_INVALID');
    feeRate = FALLBACK_REFERRAL_RATE; feeRateAssumed = true; flags.push('FEE_RATE_ASSUMED');
  }

  // 固定費。不明・異常なものがあれば「下限は計算できない」に倒す (0 で埋めると下限が低く出て、値下げを通してしまう)
  let fixedFees = 0;
  let fixedKnown = channelKnown;
  // FBA 配送代行手数料が 0 になることは無い (取得元が「不明」を 0 で埋める — Codex R4)。0 は異常として扱う
  const main = isFba ? amountState(p.fba_fee, MAX_FEE, { allowZero: false }) : amountState(p.ship_cost, MAX_FEE);
  if (!channelKnown) {
    // 発送区分が分からなければ、どの固定費を足すべきかも分からない
  } else if (main.state !== 'ok') {
    fixedKnown = false;
    flags.push(isFba ? 'FBA_FEE_UNKNOWN' : 'SHIP_UNKNOWN');
    if (main.state === 'invalid') flags.push('INPUT_INVALID');
  } else fixedFees += main.value;
  for (const extra of [amountState(p.per_item_fee, MAX_FEE), amountState(p.variable_closing_fee, MAX_FEE)]) {
    if (extra.state === 'invalid') { fixedKnown = false; flags.push('INPUT_INVALID'); }
    else if (extra.state === 'ok') fixedFees += extra.value;
  }

  let floorPrice = null;
  const denom = 1 - feeRate - minMarginRate;
  if (costKnown && fixedKnown && feeRateValid && denom > 0) {
    floorPrice = ceilDivide(cost.value + fixedFees, denom);
    if (floorPrice != null && !isValidPrice(floorPrice)) { floorPrice = null; flags.push('INPUT_INVALID'); }
  }
  return {
    costKnown, feeRate, feeRateAssumed, feeRateValid, channelKnown,
    fixedFees: fixedKnown ? fixedFees : null,
    minMarginRate, floorPrice, flags: [...new Set(flags)],
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
  // ★丸めてから検査しない (0.6 → 1 円、1899.5 → 1900 円が「妥当」になる — Codex R2)。JPY に小数は無い。
  //   小数で来た価格はデータ異常として扱う (isValidPrice が整数を要求する)
  const currentRaw = num(input.my_price);
  const current = isValidPrice(currentRaw) ? currentRaw : (currentRaw == null ? null : currentRaw);
  const grossNow = grossAt(current, costs, input.cost_incl_tax);
  // 人のストッパーは正の整数円だけ信用する (0・負・小数・巨大値は無いのと同じ)
  const userFloorRaw = num(input.floor_price);
  const userFloor = isValidPrice(userFloorRaw) ? userFloorRaw : null;
  const ceilingRaw = num(input.ceiling_price);
  const ceiling = isValidPrice(ceilingRaw) ? ceilingRaw : null;
  const offsetRaw = num(input.offset_jpy) ?? 0;
  const offset = Number.isInteger(offsetRaw) && Math.abs(offsetRaw) <= MAX_CHANGE_AMOUNT ? offsetRaw : 0;
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

  if (!isValidPrice(current)) return hold('NO_MY_PRICE', current == null ? '' : ` (${current})`);

  if (mode === 'off') {
    const extra = flags.includes('BELOW_COST_FLOOR')
      ? ` (⚠️ 今の価格 ${current.toLocaleString()} 円は計算した下限 ${costs.floorPrice.toLocaleString()} 円より低い)` : '';
    return keep('OFF', null, extra);
  }
  if (mode === 'fba_lowest' || mode === 'lowest') return hold('NO_OFFER_DATA');

  // 上限が実効下限より低い方針は矛盾。どちらを優先しても意図と違う値になるので出さない
  if (ceiling != null && effectiveFloor != null && ceiling < effectiveFloor) {
    return hold('INVALID_POLICY_BOUNDS', ` (上限 ${ceiling.toLocaleString()} 円 < 下限 ${effectiveFloor.toLocaleString()} 円)`);
  }

  // ── mode === 'buybox' ──
  const bbRaw = num(input.buybox_price);
  if (bbRaw != null && !isValidPrice(bbRaw)) return hold('NO_BUYBOX', ` (${bbRaw})`);
  const bb = bbRaw;
  const owner = input.buybox_is_mine === 1 ? 'mine' : input.buybox_is_mine === 0 ? 'other' : 'unknown';
  let target;
  let code;
  if (bb == null) {
    // カート不在でも、赤字の疑いがあれば下限まで上げる提案はできる
    if (effectiveFloor != null && current < effectiveFloor) { target = effectiveFloor; code = 'RAISE_TO_FLOOR'; }
    else return hold('NO_BUYBOX');
  } else if (owner === 'mine') {
    if (effectiveFloor != null && current < effectiveFloor) { target = effectiveFloor; code = 'RAISE_TO_FLOOR'; }
    else return keep('BUYBOX_MINE', 0.9);
  } else if (bb < current * SUSPICIOUS_BUYBOX_RATIO) {
    return hold('BUYBOX_SUSPICIOUS', ` (カート ${bb.toLocaleString()} 円 / 自分 ${current.toLocaleString()} 円)`);
  } else {
    target = bb + offset;
    code = 'MATCH_BUYBOX';
    // 持ち主が分からないカートは自社かもしれない。追いかけて下げると自分を下回り続ける → 値下げは出さない
    if (owner === 'unknown' && target < current) {
      if (effectiveFloor != null && current < effectiveFloor) { target = effectiveFloor; code = 'RAISE_TO_FLOOR'; }
      else return hold('BUYBOX_OWNER_UNKNOWN', ` (カート ${bb.toLocaleString()} 円)`);
    }
  }

  // 下限・上限で止める
  if (effectiveFloor != null && target < effectiveFloor) {
    target = effectiveFloor; code = 'FLOOR_CLAMP'; flags.push('FLOOR_CLAMP');
  }
  if (costs.floorPrice == null && target < current) {
    // ★下限を**計算できない**行の値下げは出さない (事故ルール 2)。人のストッパーがあっても免除しない —
    //   原価・手数料を知らずに「ここまでなら下げてよい」とは言えない (Codex R4 High。旧ツールは人の入力だけで下げた)
    return hold('NO_FLOOR', ` (カートは ${bb == null ? '無し' : bb.toLocaleString() + ' 円'})`);
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

  const action = target > current ? 'raise' : 'lower';
  // ★最終の不変条件 (どの経路でも): 値下げの提案は「計算した下限がある」かつ「実効下限以上」。ここに来たらルールの不具合なので出さずに止める
  if (action === 'lower' && (costs.floorPrice == null || effectiveFloor == null || target < effectiveFloor)) {
    return { ...hold('FLOOR_INVARIANT', ` (${target} / 下限 ${effectiveFloor} / 計算 ${costs.floorPrice})`), targetPrice: target, changeRatio };
  }
  // ★最終の不変条件 (Codex R2 High): 持ち主不明のカートでは値下げしない。上限クランプで値上げが値下げに反転する経路も含む
  if (action === 'lower' && owner === 'unknown') {
    return { ...hold('BUYBOX_OWNER_UNKNOWN', ` (カート ${bb == null ? '無し' : bb.toLocaleString() + ' 円'}、上限で反転)`), targetPrice: target, changeRatio };
  }
  // ★最終の不変条件 (Codex R3 High): 価格計算に関わる入力に異常値があれば値下げしない (下限が信用できない)
  if (action === 'lower' && (flags.includes('INPUT_INVALID') || flags.includes('CHANNEL_UNKNOWN'))) {
    return { ...hold('NO_FLOOR', ' (入力に異常値があるため)'), targetPrice: target, changeRatio };
  }

  const confidence = code === 'RAISE_TO_FLOOR' ? 0.9
    : code === 'MATCH_BUYBOX' ? (costs.feeRateAssumed ? 0.6 : 0.8)
      : 0.7; // FLOOR_CLAMP / CEILING_CLAMP
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
