/**
 * price-update / 利益計算とガード判定 (要件定義 v1.0 F3・F4)
 *
 * ★2系統に分けてある (要件 F3):
 *   (a) 参考表示の概算粗利 — モール手数料の近似 (dim_mall.fee_rate_approx) を引いた「管理近似」。
 *       請求実額ではないので、これで更新可否を決めない。
 *   (b) 更新ブロック用の原価割れ判定 — 手数料を使わない保守式。
 *       「原価(税込換算) + 配送関係費」を下回ったらブロック。手数料の誤差に判定を依存させない。
 *
 * ここは純関数だけ (DB も fetch も触らない)。M1 は表示のみだが、M2 以降の実行側も同じ関数を通す
 * (画面のチェックだけだと API 直叩きで抜けられる — 要件 F4「ガードはすべてサーバ側強制」)。
 *
 * 金額の扱い:
 *   ・モールの設定価格は税込・整数円 (モールデータは税込みが家ルール)
 *   ・原価 (m_products.原価) は税抜 → 判定では ×(1+消費税率) して税込相当にそろえる
 *   ・消費税率は m_products.消費税率 = 0.10 / 0.08 の小数。未登録は 10% 扱い (他アプリと同じ)
 */

/** 消費税率が未登録なら 10% (amazon-accounting と同じ扱い) */
export const DEFAULT_TAX_RATE = 0.10;
/** 楽天APIの許容上限。これを超える値は送る前に落とす */
export const MAX_PRICE = 999_999_999;
/** 変更率の許容幅 (決定事項#2)。値下げ −30% / 値上げ +100% */
export const MIN_CHANGE_RATIO = -0.30;
export const MAX_CHANGE_RATIO = 1.00;
/** 変更額の外周ガード。1商品で +10万円を超える値上げは誤入力を疑う */
export const MAX_CHANGE_AMOUNT = 100_000;
/** 粗利率がこれ未満なら警告 (ブロックはしない) */
export const LOW_MARGIN_RATE = 0.10;

/** 整数円として妥当か (正の整数・上限内)。0 円は楽天APIが 204 で通してしまうのでここで弾く */
export function isValidPrice(v) {
  return Number.isInteger(v) && v > 0 && v <= MAX_PRICE;
}

/**
 * 数値化。★null / undefined / 空文字は null のまま返す。
 * Number(null) は 0 になるので、素の Number() を使うと「原価未登録」が「原価0円」に化け、
 * 原価割れ判定をすり抜けてしまう (このツールで一番やってはいけない事故)。
 */
function num(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * 概算粗利 (参考表示用)。
 * @param {{price:number, cost:number|null, taxRate:number|null, feeRate:number, shipping:number|null}} p
 * @returns {{gross:number|null, rate:number|null, fee:number, costInclTax:number|null, shipping:number}}
 *   cost が無い商品は gross=null (「原価未登録」として表示側で区別する。0 円原価として計算しない)
 */
export function estimateGross({ price, cost, taxRate, feeRate, shipping }) {
  const p = num(price);
  const c = num(cost);
  const t = num(taxRate) ?? DEFAULT_TAX_RATE;
  const f = num(feeRate) ?? 0;
  const s = num(shipping) ?? 0;
  const fee = p == null ? 0 : Math.round(p * f);
  if (p == null || c == null) {
    return { gross: null, rate: null, fee, costInclTax: c == null ? null : c * (1 + t), shipping: s };
  }
  const costInclTax = c * (1 + t);
  const gross = p - costInclTax - fee - s;
  return { gross, rate: p > 0 ? gross / p : null, fee, costInclTax, shipping: s };
}

/**
 * 原価割れ判定 (ブロック用・保守側)。手数料は使わない。
 * @returns {{ok:boolean, floor:number|null, reason:string|null}}
 *   原価が無い行は判定できない → ok:false (fail-closed。原価未登録のまま値付けさせない)
 */
export function costFloorCheck({ price, cost, taxRate, shipping }) {
  const p = num(price);
  const c = num(cost);
  const t = num(taxRate) ?? DEFAULT_TAX_RATE;
  const s = num(shipping) ?? 0;
  if (c == null) return { ok: false, floor: null, reason: '原価が未登録のため原価割れを判定できません' };
  const floor = c * (1 + t) + s;
  if (p == null) return { ok: false, floor, reason: '新売価が未入力です' };
  if (p < floor) return { ok: false, floor, reason: `原価割れ (最低 ${Math.ceil(floor).toLocaleString()} 円)` };
  return { ok: true, floor, reason: null };
}

/**
 * 1行ぶんのガード評価 (要件 F4)。M1 は表示のみ、M2 以降は実行前にも必ずこれを通す。
 *
 * @param {object} row
 * @param {number|null} row.currentPrice   ライブの設定価格 (取れていない行は更新不可)
 * @param {number|null} row.newPrice       入力された新売価
 * @param {number|null} row.cost           原価 (税抜)
 * @param {number|null} row.taxRate
 * @param {number|null} row.shipping
 * @param {number} row.feeRate
 * @param {string} row.mall
 * @param {string} row.confidence          引き当て信頼度
 * @param {boolean} [row.isRecovery]       復旧 run か (変更率ガードだけ免除される)
 * @returns {{blocks:string[], warns:string[], canUpdate:boolean, changeRatio:number|null,
 *            changeAmount:number|null, estimate:object, floor:object}}
 */
export function evaluateRow(row) {
  const blocks = [];
  const warns = [];
  const mall = String(row.mall || '');
  const current = num(row.currentPrice);
  const next = num(row.newPrice);

  // Amazon はデータの側でも更新対象から外す (決定事項⑥。画面で選べないだけでは直叩きで突破される)
  if (mall === 'amazon') blocks.push('Amazon は本ツールの更新対象外です (既存の価格管理の仕組みを使ってください)');
  if (mall === 'aupay' || mall === 'qoo10') blocks.push('このモールは手動更新です (API更新は Phase 2)');

  if (row.confidence !== 'confirmed') blocks.push('引き当てが確定していない行は更新できません');
  if (current == null) {
    blocks.push('現在の設定価格を取得できていない行は更新できません');
  } else if (!isValidPrice(current)) {
    // ★現在価格が 0 や負数だと変更率ガードが素通りする (0 で割れないので比較が飛ばされる)。
    // モール側が 0 円で登録されている異常状態のまま値付けさせない (Codex R1 High)
    blocks.push(`現在の設定価格が異常です (${current})。モール側を確認してください`);
  }

  if (next == null) {
    // 新売価未入力は「まだ対象でない」だけ。ブロック理由には積むが警告は出さない
    blocks.push('新売価が未入力です');
  } else if (!isValidPrice(next)) {
    blocks.push(`新売価は 1〜${MAX_PRICE.toLocaleString()} の整数円で入力してください (0円は不可)`);
  }

  let changeRatio = null;
  let changeAmount = null;
  if (isValidPrice(current) && next != null && isValidPrice(next)) {
    changeAmount = next - current;
    changeRatio = current > 0 ? changeAmount / current : null;
    if (changeRatio != null && !row.isRecovery) {
      if (changeRatio < MIN_CHANGE_RATIO) {
        blocks.push(`値下げ幅が大きすぎます (${(changeRatio * 100).toFixed(1)}% / 許容 ${MIN_CHANGE_RATIO * 100}% まで)`);
      }
      if (changeRatio > MAX_CHANGE_RATIO) {
        blocks.push(`値上げ幅が大きすぎます (${(changeRatio * 100).toFixed(1)}% / 許容 +${MAX_CHANGE_RATIO * 100}% まで)`);
      }
    }
    if (changeAmount > MAX_CHANGE_AMOUNT) {
      blocks.push(`変更額が大きすぎます (+${changeAmount.toLocaleString()} 円 / 許容 +${MAX_CHANGE_AMOUNT.toLocaleString()} 円)`);
    }
  }

  const floor = costFloorCheck({ price: next, cost: row.cost, taxRate: row.taxRate, shipping: row.shipping });
  if (next != null && !floor.ok && floor.reason) blocks.push(floor.reason);

  const estimate = estimateGross({
    price: next ?? current, cost: row.cost, taxRate: row.taxRate,
    feeRate: row.feeRate, shipping: row.shipping,
  });
  if (estimate.rate != null && estimate.rate < LOW_MARGIN_RATE) {
    warns.push(`粗利率が低いです (概算 ${(estimate.rate * 100).toFixed(1)}%)`);
  }

  return {
    blocks, warns,
    canUpdate: blocks.length === 0,
    changeRatio, changeAmount,
    estimate, floor,
  };
}

/** run 全体の上限 (要件 F4-7)。env で下げられる (M2 初期は 5 コード) */
export function runLimits(env = process.env) {
  const n = (v, d) => {
    const x = parseInt(v, 10);
    return Number.isInteger(x) && x > 0 ? x : d;
  };
  return {
    maxNeCodes: n(env.PRICE_UPDATE_MAX_NE_CODES, 20),
    maxSkuRows: n(env.PRICE_UPDATE_MAX_SKU_ROWS, 100),
    maxRowsPerNeCode: 50,
  };
}
